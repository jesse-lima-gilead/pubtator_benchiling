import math
import uuid
from typing import Dict, List
from collections import Counter
from transformers import AutoTokenizer
from src.pubtator_utils.db_handler.alembic_models.chunks_with_annotations import (
    ChunkWithAnnotations,
)  # Import the Chunk model
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.db_handler.db import session  # Import the session

from src.data_processing.chunking.chunks_handler import (
    chunk_annotated_articles,
)
from src.pubtator_utils.vector_db_handler.vector_db_handler_factory import (
    VectorDBHandler,
)
from src.data_processing.merging.merge_handler import merge_annotations
from src.data_processing.embedding.embeddings_handler import (
    get_embeddings,
    get_model_info,
    save_embeddings_details_to_json,
)
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
# # Docker Qdrant
# vectordb_config = config_loader.get_config("vectordb")["qdrant"]
# # Cloud Qdrant
# vectordb_config = config_loader.get_config("vectordb")["qdrant_cloud"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class ArticleProcessor:
    def __init__(
        self,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        aioner_model: str = "Bioformer",
        gnorm2_model: str = "Bioformer",
        embeddings_model: str = "pubmedbert",
        chunker: str = "sliding_window",
        merger: str = "prepend",
    ):
        self.aioner_model = aioner_model
        self.gnorm2_model = gnorm2_model
        self.embeddings_model = embeddings_model
        self.chunker = chunker
        self.merger = merger
        self.articles_input_dir = paths_config["annotations_merged_path"]
        self.articles_summary_dir = paths_config["summary_path"]
        self.chunks_output_dir = paths_config["chunks_path"]
        self.embeddings_output_dir = paths_config["embeddings_path"]
        self.articles_metadata_dir = paths_config["metadata_path"]
        self.file_handler = file_handler
        # self.s3_io_util = S3IOUtil()

    def get_article_summary(self, article_file):
        # ToDo: Put the article summariser in data ingestion orchestrator
        article_file_name = f"{article_file.split('.')[0]}.txt"
        logger.info(f"Fetching article {article_file_name} summary")
        article_file_summary_path = self.file_handler.get_file_path(
            self.articles_summary_dir, article_file_name
        )
        # article_file_summary_path = (
        #     f"{self.articles_summary_dir}/{article_file.split('.')[0]}.txt"
        # )
        # logger.info(f"Article summary file path: {article_file_summary_path}")
        article_summary = self.file_handler.read_file(article_file_summary_path)
        # with open(article_file_summary_path, "r") as f:
        #     return f.read()
        logger.info(f"Article summary: {article_summary}")
        return article_summary

    def get_token_count(self, chunk_text: str):
        model_info = get_model_info(self.embeddings_model)
        model_path = model_info[0]
        tokenizer = AutoTokenizer.from_pretrained(model_path)
        return len(tokenizer.tokenize(chunk_text))

    def get_words_count(self, chunk_text: str):
        return len(chunk_text.split())

    def get_article_chunks(self, input_file_path: str, article_file: str):
        logger.info(f"Chunking article {article_file}")

        if (
            self.chunker == "sliding_window"
            or self.chunker == "grouped_annotation_aware_sliding_window"
        ):
            # For Actual Processing using Sliding Window
            # Getting Dynamic Window size based on the article summary
            summary = self.get_article_summary(article_file)
            summary_tokens = self.get_token_count(summary)
            model_info = get_model_info(self.embeddings_model)
            max_tokens = model_info[1]
            tokens_left = max_tokens - summary_tokens
            buffer = math.floor(tokens_left * 0.15)
            tokens_left_with_buffer = tokens_left - buffer
            words_left = math.floor(tokens_left_with_buffer * 0.75)
            window_size = 2 * words_left

            # For Baseline Processing
            # window_size = 512
            logger.info(f"Dynamic Window Size for chunking: {window_size}")

            chunks = chunk_annotated_articles(
                file_handler=self.file_handler,
                input_file_path=input_file_path,
                chunker_type=self.chunker,
                window_size=window_size,
            )

        else:
            chunks = chunk_annotated_articles(
                file_handler=self.file_handler,
                input_file_path=input_file_path,
                chunker_type=self.chunker,
            )

        return chunks

    def get_chunks_with_merged_annotations(
        self, input_file_path: str, article_file: str
    ):
        chunks = self.get_article_chunks(
            input_file_path=input_file_path, article_file=article_file
        )
        logger.info("Merging annotations with text of chunks")
        chunks_with_merged_annotations = []
        for i, chunk in enumerate(chunks):
            merged_text = merge_annotations(
                text=chunk["text"],
                annotations=chunk["annotations"],
                merger_type=self.merger,
            )
            chunk["merged_text"] = merged_text
            chunks_with_merged_annotations.append(chunk)

        return chunks_with_merged_annotations

    def get_chunks_with_summary(self, input_file_path: str, article_file: str):
        summary = self.get_article_summary(article_file)
        chunks = self.get_chunks_with_merged_annotations(
            input_file_path=input_file_path, article_file=article_file
        )
        chunks_with_summary = []
        logger.info("Adding article summary to chunks")
        for i, chunk in enumerate(chunks):
            chunk["summary"] = summary
            chunk[
                "merged_text_with_summary"
            ] = f"Summary:\n{summary}\n{chunk['merged_text']}"
            chunks_with_summary.append(chunk)

        return chunks_with_summary

    def calculate_annotations_per_bioconcept(
        self, chunk_annotations: List[Dict]
    ) -> Dict[str, int]:
        """
        Calculates the count of annotations per bioconcept.

        Args:
            chunk_annotations (List[Dict]): List of annotation dictionaries.

        Returns:
            Dict[str, int]: Dictionary with counts of each bioconcept, including 0 for those not present.
        """
        # List of bioconcepts to calculate counts for
        # Predefined bioconcepts
        bioconcepts = {
            "Gene",
            "Species",
            "Strain",
            "Genus",
            "CellLine",
            "Disease",
            "Chemical",
        }

        # Initialize a Counter to track counts
        type_counts = Counter()

        # Count annotations per bioconcept, classifying unknown types as "Variant"
        for annotation in chunk_annotations:
            annotation_type = annotation.get("type")
            if annotation_type in bioconcepts:
                type_counts[annotation_type] += 1
            else:
                type_counts["Variant"] += 1  # Classify all other types as "Variant"

        # Ensure all bioconcepts are represented in the output with 0 if not present
        result = {
            concept: type_counts.get(concept, 0)
            for concept in bioconcepts.union({"Variant"})
        }
        return result

    def process_chunks(self):
        for article_file in self.file_handler.list_files(self.articles_input_dir):
            if article_file.endswith(".xml"):
                logger.info(f"Processing article {article_file}...")
                input_file_path = self.file_handler.get_file_path(
                    self.articles_input_dir, article_file
                )
                chunk_output_file_name = f"{article_file.split('.')[0]}.json"
                chunks_output_path = self.file_handler.get_file_path(
                    self.chunks_output_dir, chunk_output_file_name
                )
                article_id = article_file.split(".")[0]
                article_metadata_file_name = f"{article_id}_metadata.json"
                article_metadata_file_path = self.file_handler.get_file_path(
                    self.articles_metadata_dir, article_metadata_file_name
                )
                article_metadata_json = self.file_handler.read_json_file(
                    article_metadata_file_path
                )
                # For Actual Processing
                chunks = self.get_chunks_with_summary(
                    input_file_path=input_file_path, article_file=article_file
                )
                all_chunk_details = []

                for i, chunk in enumerate(chunks):
                    chunk_id = str(uuid.uuid4())
                    chunk_sequence = f"{i + 1}"
                    chunk_name = f"{article_id}_chunk_{chunk_sequence}"
                    article_summary = chunk["summary"]
                    article_id = article_id
                    chunk_text = chunk["text"]
                    merged_text = chunk["merged_text"]
                    merged_text_with_summary = chunk["merged_text_with_summary"]
                    chunk_annotations = chunk["annotations"]
                    annotations_per_bioconcept = (
                        self.calculate_annotations_per_bioconcept(chunk_annotations)
                    )
                    chunk_length = self.get_token_count(chunk_text)
                    # token_count = len(merged_text_with_summary.split())
                    token_count = self.get_token_count(merged_text_with_summary)
                    chunk_annotations_count = len(chunk_annotations)
                    chunk_annotations_ids = [ann["id"] for ann in chunk_annotations]
                    chunk_offset = chunk["offset"]
                    chunk_infons = chunk["infons"]
                    chunker_type = self.chunker
                    merger_type = self.merger
                    embeddings_model = self.embeddings_model
                    aioner_model = self.aioner_model
                    gnorm2_model = self.gnorm2_model

                    chunk_details = {
                        "chunk_sequence": chunk_sequence,
                        "merged_text": merged_text_with_summary,
                        "payload": {
                            "chunk_id": chunk_id,
                            "chunk_name": chunk_name,
                            "chunk_text": chunk_text,
                            # "chunk_annotations": chunk_annotations,
                            "chunk_length": chunk_length,
                            "token_count": token_count,
                            "chunk_annotations_count": chunk_annotations_count,
                            # "chunk_annotations_ids": chunk_annotations_ids,
                            "genes": annotations_per_bioconcept["Gene"],
                            "species": annotations_per_bioconcept["Species"],
                            "strains": annotations_per_bioconcept["Strain"],
                            "genus": annotations_per_bioconcept["Genus"],
                            "cell_lines": annotations_per_bioconcept["CellLine"],
                            "diseases": annotations_per_bioconcept["Disease"],
                            "chemicals": annotations_per_bioconcept["Chemical"],
                            "variants": annotations_per_bioconcept["Variant"],
                            # "chunk_offset": chunk_offset,
                            # "chunk_infons": chunk_infons,
                            # "chunker_type": chunker_type,
                            # "merger_type": merger_type,
                            # "embeddings_model": embeddings_model,
                            # "aioner_model": aioner_model,
                            # "gnorm2_model": gnorm2_model,
                            "article_id": article_id,
                            "article_summary": article_summary,
                        },
                    }

                    # Add metadata to the payload
                    for key, value in article_metadata_json.items():
                        chunk_details["payload"][key] = value

                    all_chunk_details.append(chunk_details)

                    # # Insert into PostgreSQL
                    # chunk_record = ChunkWithAnnotations(
                    #     article_id=article_id,
                    #     chunk_id=chunk_id,
                    #     chunk_sequence=chunk_sequence,
                    #     chunk_name=chunk_name,
                    #     chunk_length=chunk_length,
                    #     token_count=token_count,
                    #     chunk_annotations_count=chunk_annotations_count,
                    #     chunk_annotations_ids=chunk_annotations_ids,
                    #     genes=annotations_per_bioconcept["Gene"],
                    #     species=annotations_per_bioconcept["Species"],
                    #     cell_lines=annotations_per_bioconcept["CellLine"],
                    #     strains=annotations_per_bioconcept["Strain"],
                    #     diseases=annotations_per_bioconcept["Disease"],
                    #     chemicals=annotations_per_bioconcept["Chemical"],
                    #     variants=annotations_per_bioconcept["Variant"],
                    #     chunk_offset=chunk_offset,
                    #     chunk_infons=chunk_infons,
                    #     chunker_type=chunker_type,
                    #     merger_type=merger_type,
                    #     aioner_model=aioner_model,
                    #     gnorm2_model=gnorm2_model,
                    # )
                    # session.add(chunk_record)
                    # session.commit()

                # Save chunks to file
                self.file_handler.write_file_as_json(
                    chunks_output_path, all_chunk_details
                )
                logger.info(f"Chunks file saved to {chunks_output_path}")

    def get_chunks_embeddings_details(self, chunks: List[Dict], chunk_file_path: str):
        try:
            logger.info("Generating embeddings for the chunks")
            chunk_texts = []
            for chunk in chunks:
                chunk_texts.append(
                    chunk["merged_text"]
                    if collection_type == "processed_pubmedbert"
                    else chunk["payload"]["chunk_text"]
                )

            embeddings = get_embeddings(
                model_name=self.embeddings_model,
                texts=chunk_texts,
            )

            chunk_embedding_payload = []
            for idx, chunk in enumerate(chunks):
                cur_chunk_dic = {}
                chunk_payload = chunk["payload"]
                chunk_payload["merged_text"] = chunk["merged_text"]
                cur_chunk_dic["payload"] = chunk_payload
                cur_chunk_dic["embeddings"] = embeddings[idx].tolist()
                chunk_embedding_payload.append(cur_chunk_dic)

            return chunk_embedding_payload

        except Exception as e:
            logger.error(f"Error while processing chunk: {e}")
            raise e

    def store_embeddings_details_in_file(
        self, embeddings_details, embeddings_filename: str
    ):
        # Write the Embeddings to a file:
        embeddings_file_path = self.file_handler.get_file_path(
            self.embeddings_output_dir, embeddings_filename
        )
        logger.info(f"Saving embeddings to file: {embeddings_file_path}")
        # embeddings_file_path = f"{self.embeddings_output_dir}/{embeddings_filename}"
        save_embeddings_details_to_json(
            embeddings_details_list=embeddings_details,
            filename=embeddings_file_path,
            file_handler=self.file_handler,
        )

    def store_embeddings_details_at_vectordb(
        self,
        collection_type: str,
        chunk_file_path: str,
        vector_db_type: str = None,
        vector_db_params: dict = None,
        index_params: dict = None,
    ):
        # Get the Vector DB Handler with for specific vector db config
        vector_db_handler = VectorDBHandler(
            vector_db_params=vector_db_params, index_params=index_params
        )
        vector_db_manager = vector_db_handler.get_vector_db_manager(
            vector_db_type=vector_db_type
        )

        # batch_size = 10  # Adjust batch size based on performance
        # batch = []

        logger.info("Putting the embeddings in QdrantDB")
        # Load the chunks file from local:
        chunks = self.file_handler.read_json_file(chunk_file_path)
        for chunk in chunks:
            # model_info = get_model_info(self.embeddings_model)
            # logger.info("Generating embeddings for the chunk")
            chunk_embeddings = get_embeddings(
                model_name=self.embeddings_model,
                texts=[
                    chunk["merged_text"]
                    if collection_type == "processed_pubmedbert"
                    else chunk["payload"]["chunk_text"]
                ],
            )[0]
            # logger.info("Embedding generated!")
            chunk_payload = chunk["payload"]
            chunk_payload["merged_text"] = chunk["merged_text"]

            # Insert into Vector DB
            vector_db_manager.insert_vector(
                vector=chunk_embeddings, payload=chunk_payload
            )

    def process_embeddings(
        self,
        # embeddings_output_dir: str,
        collection_type: str,
        store_embeddings_as_file,
        vector_db_type: str = None,
        vector_db_params: dict = None,
        index_params: dict = None,
    ):
        # Load the chunks file:
        for chunks_file in self.file_handler.list_files(self.chunks_output_dir):
            if chunks_file.endswith(".json"):
                logger.info(f"Processing chunks file {chunks_file}...")
                chunk_file_path = self.file_handler.get_file_path(
                    self.chunks_output_dir, chunks_file
                )
                # chunk_file_path = f"{self.chunks_output_dir}/{chunks_file}"
                chunks = self.file_handler.read_json_file(chunk_file_path)
                if store_embeddings_as_file:
                    embeddings_details = self.get_chunks_embeddings_details(
                        chunks=chunks, chunk_file_path=chunk_file_path
                    )
                    # print(f"Embedding details in process_embeddings(): {embeddings_details}")
                    self.store_embeddings_details_in_file(
                        embeddings_details=embeddings_details,
                        embeddings_filename=f"{chunks_file.split('.')[0]}_embeddings.json",
                    )
                else:
                    self.store_embeddings_details_at_vectordb(
                        collection_type=collection_type,
                        chunk_file_path=chunk_file_path,
                        vector_db_type=vector_db_type,
                        vector_db_params=vector_db_params,
                        index_params=index_params,
                    )

    def process(
        self,
        collection_type: str,
        store_embeddings_as_file: bool = True,
    ):
        logger.info("Creating Chunks...")
        self.process_chunks()
        logger.info("Chunks created successfully!")

        logger.info("Creating and storing embeddings...")
        # Create Embeddings and store them locally or in vectorDB
        self.process_embeddings(
            # embeddings_output_dir=self.embeddings_output_dir,
            collection_type=collection_type,
            store_embeddings_as_file=store_embeddings_as_file,
        )
        logger.info("Embeddings stored successfully")


def run(
    run_type: str = "all",
    collection_type: str = "processed_pubmedbert",
    store_embeddings_as_file: bool = True,
):
    embeddings_model = "pubmedbert"
    chunker = "sliding_window"
    merger = "prepend"

    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    article_processor = ArticleProcessor(
        embeddings_model=embeddings_model,
        chunker=chunker,
        merger=merger,
        file_handler=file_handler,
        paths_config=paths,
    )

    if run_type == "all":
        article_processor.process(
            collection_type=collection_type,
            store_embeddings_as_file=True,
        )
    elif run_type == "chunks":
        article_processor.process_chunks()
    elif run_type == "embeddings":
        if store_embeddings_as_file:
            article_processor.process_embeddings(
                collection_type=collection_type,
                store_embeddings_as_file=store_embeddings_as_file,
            )
        else:
            # Retrieve vector db specific config
            vectordb_config = config_loader.get_config("vectordb")["vector_db"]
            vector_db_type = vectordb_config["type"]
            vector_db_params = vectordb_config[vector_db_type]["vector_db_params"]
            index_params = vectordb_config[vector_db_type]["index_params"][
                collection_type
            ]

            article_processor.process_embeddings(
                collection_type=collection_type,
                store_embeddings_as_file=store_embeddings_as_file,
                vector_db_type=vector_db_type,
                vector_db_params=vector_db_params,
                index_params=index_params,
            )
    else:
        logger.error(f"Invalid run type: {run_type}")


if __name__ == "__main__":
    # Processed Text Collection
    collection_type = "processed_pubmedbert"

    # Baseline Text Collection
    # collection_type = "baseline"

    # Test Collection
    # collection_type = "test"

    store_embeddings_as_file = True
    run_type = "all"  # "all" or "chunks" or "embeddings"
    run(
        run_type=run_type,
        collection_type=collection_type,
        store_embeddings_as_file=store_embeddings_as_file,
    )
