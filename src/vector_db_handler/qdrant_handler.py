import logging
from typing import Any, Dict

from vector_db_handler.qdrant_manager import QdrantManager
from src.llm_handler.llm_factory import LLMFactory


class QdrantHandler:
    def __init__(
        self,
        chunker_type: str,
        merger_type: str,
        ner_model: str,
        qdrant_config: Dict,
        llm_model: str,
    ):
        """
        Initializes the QdrantHandler with Qdrant manager, ChatBedrock model, and memory limit.

        Args:
            chunker_type (str): Type of text chunker used.
            merger_type (str): Type of text merger used.
            ner_model (str): Model used for NER.
            qdrant_config (Dict): Qdrant manager configuration dictionary.
            llm_model (str): The LLM model to use.
        """
        self.chunker_type = chunker_type
        self.merger_type = merger_type
        self.ner_model = ner_model
        self.qdrant_manager = self._create_qdrant_manager(qdrant_config)

        # Initialize LLMs
        self.llm_model = llm_model
        llm_factory = LLMFactory()
        llm_handler = llm_factory.create_llm(llm_type=self.llm_model)
        self.query_llm = llm_handler.get_query_llm()
        self.embed_llm = llm_handler.get_embed_llm()

    def _create_qdrant_manager(self, qdrant_config: Dict) -> QdrantManager:
        """Creates a QdrantManager instance using the configuration."""
        try:
            params = qdrant_config["collections"][self.chunker_type][self.merger_type][
                self.ner_model
            ]
            return QdrantManager(
                host=qdrant_config["host"],
                port=qdrant_config["port"],
                collection_name=params["collection_name"],
                vector_size=params["vector_size"],
                distance_metric=params["distance_metric"],
            )
        except KeyError as e:
            raise ValueError(f"Configuration missing for: {e}")

    def retrieve_query(self, query: str, limit: int = 1):
        """
        Retrieves related data to the user's question by querying the vector store.

        Args:
            query (str): The user's query.
            limit (int, optional): Defaults to 1. The no. of closest chunks to be returned.
        """
        logging.info(f"Processing query: {query}")

        # Generate the embedding for the query
        query_embedding = self.embed_llm.embed_query(query)

        # Search the vector store for relevant vectors
        search_results = self.qdrant_manager.search_vectors(query_embedding, limit)

        logging.info(f"Retrieved results: {search_results}")

        # Extract relevant texts from the search results
        retrieved_chunks = [result.payload["chunk_text"] for result in search_results]

        # Combine the relevant texts into a context for the LLM
        retrieved_chunks = "\n".join(retrieved_chunks)

        logging.info(f"Retrieved chunks: {retrieved_chunks}")

        return search_results

    def insert_chunk(self, chunk: Dict[str, Any]):
        """
        Method to process each chunk and store it in a vector db.
        Args:
            chunk: Data to be inserted into the qdrant collections
        """
        # Prepare the payload
        chunk_payload = chunk["payload"]
        chunk_payload["chunk_text"] = chunk["chunk_text"]
        chunk_payload["chunk_annotations"] = str(chunk["chunk_annotations"])
        chunk_id = chunk_payload["chunk_id"]

        logging.info(
            f"Processing chunk {chunk_id} for inserting into collection: {self.qdrant_manager.collection_name}"
        )

        # Get the details for inserting into vector db
        chunk_enriched_txt = chunk["merged_text"]

        # Generate the embedding for the chunk text
        embedding = self.embed_llm.embed_query(chunk_enriched_txt)

        if not self.qdrant_manager.check_if_collection_exists():
            self.qdrant_manager.create_collection()

        # insert into qdrant collection
        self.qdrant_manager.insert_vector(embedding, chunk_payload)

        logging.info(
            f"Inserted chunk {chunk_id} into collection: {self.qdrant_manager.collection_name}"
        )
