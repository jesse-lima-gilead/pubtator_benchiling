import json
import math
import os
import uuid
from typing import Dict, List
from transformers import AutoTokenizer
from src.alembic_models.chunks import Chunk  # Import the Chunk model

# from src.utils.s3_io_util import S3IOUtil
from src.data_processing.chunking.chunks_handler import (
    chunk_annotated_articles,
    write_chunks_details_to_file,
)
from src.vector_db_handler.qdrant_handler import QdrantHandler
from src.data_processing.merging.merge_handler import merge_annotations
from src.data_processing.embedding.embeddings_handler import (
    get_embeddings,
    get_model_info,
    save_embeddings_details_to_json,
)
from src.utils.config_reader import YAMLConfigLoader
from src.utils.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
vectordb_config = config_loader.get_config("vectordb")["qdrant"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class ArticleProcessor:
    def __init__(
        self,
        aioner_model: str = "Bioformer",
        gnorm2_model: str = "Bioformer",
        embeddings_model: str = "pubmedbert",
        chunker: str = "sliding_window",
        merger: str = "prepend",
        articles_input_dir: str = "../../data/ner_processed/gnorm2_annotated/",
        articles_summary_dir: str = "../../data/articles_metadata/article_summaries/",
        chunks_output_dir: str = "../../data/indexing/chunks/",
    ):
        self.aioner_model = aioner_model
        self.gnorm2_model = gnorm2_model
        self.embeddings_model = embeddings_model
        self.chunker = chunker
        self.merger = merger
        self.articles_input_dir = articles_input_dir
        self.articles_summary_dir = articles_summary_dir
        self.chunks_output_dir = chunks_output_dir

    def get_article_summary(self, article_file):
        # ToDo: Put the article summariser in data ingestion orchestrator
        logger.info(f"Fetching article {article_file} summary")
        article_file_summary_path = (
            f"{self.articles_summary_dir}/{article_file.split('.')[0]}.txt"
        )
        # logger.info(f"Article summary file path: {article_file_summary_path}")
        with open(article_file_summary_path, "r") as f:
            return f.read()

    def get_token_count(self, chunk_text: str):
        model_info = get_model_info(self.embeddings_model)
        model_path = model_info[0]
        tokenizer = AutoTokenizer.from_pretrained(model_path)
        return len(tokenizer.tokenize(chunk_text))

    def get_words_count(self, chunk_text: str):
        return len(chunk_text.split())

    def get_article_chunks(self, input_file_path: str, article_file: str):
        logger.info(f"Chunking article {article_file}")

        # For Actual Processing
        summary = self.get_article_summary(article_file)
        summary_words = self.get_words_count(summary)
        max_tokens = 512
        tokens_left = max_tokens - math.floor(summary_words * 1.34)
        buffer = math.floor(tokens_left * 0.15)
        tokens_left_with_buffer = tokens_left - buffer
        words_left = math.floor(tokens_left_with_buffer * 0.75)
        window_size = 2 * words_left

        # For Baseline Processing
        # window_size = 512
        logger.info(f"Dynamic Window Size for chunking: {window_size}")

        chunks = chunk_annotated_articles(
            input_file_path=input_file_path,
            chunker_type=self.chunker,
            window_size=window_size,
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
            ] = f"Summary:\n{summary}\nText:\n{chunk['merged_text']}"
            chunks_with_summary.append(chunk)

        return chunks_with_summary

    def process_chunks(self):
        for article_file in os.listdir(self.articles_input_dir):
            if article_file.endswith(".xml"):
                logger.info(f"Processing article {article_file}...")
                input_file_path = f"{self.articles_input_dir}/{article_file}"
                chunks_output_path = (
                    f"{self.chunks_output_dir}/{article_file.split('.')[0]}.json"
                )
                article_id = article_file.split(".")[0]

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
                    chunk_length = len(chunk_text)
                    # token_count = len(merged_text_with_summary.split())
                    token_count = self.get_token_count(merged_text_with_summary)
                    chunk_annotations_count = len(chunk_annotations)
                    chunk_annotations_ids = [ann["id"] for ann in chunk_annotations]
                    chunk_annotations_types = list(
                        set([ann["type"] for ann in chunk_annotations])
                    )
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
                            "chunk_annotations": chunk_annotations,
                            "chunk_length": chunk_length,
                            "token_count": token_count,
                            "chunk_annotations_count": chunk_annotations_count,
                            "chunk_annotations_ids": chunk_annotations_ids,
                            "chunk_annotations_types": chunk_annotations_types,
                            "chunk_offset": chunk_offset,
                            "chunk_infons": chunk_infons,
                            "chunker_type": chunker_type,
                            "merger_type": merger_type,
                            "embeddings_model": embeddings_model,
                            "aioner_model": aioner_model,
                            "gnorm2_model": gnorm2_model,
                            "article_id": article_id,
                            "article_summary": article_summary,
                        },
                    }

                    all_chunk_details.append(chunk_details)

                    # Insert into PostgreSQL
                    chunk_record = Chunk(
                        chunk_id=chunk_id,
                        chunk_sequence=chunk_sequence,
                        chunk_name=chunk_name,
                        chunk_length=chunk_length,
                        token_count=token_count,
                        chunk_annotations_count=chunk_annotations_count,
                        chunk_annotations_ids=chunk_annotations_ids,
                        chunk_annotations_types=chunk_annotations_types,
                        chunk_offset=chunk_offset,
                        chunk_infons=chunk_infons,
                        chunker_type=chunker_type,
                        merger_type=merger_type,
                        aioner_model=aioner_model,
                        gnorm2_model=gnorm2_model,
                        article_id=article_id,
                    )
                    # session.add(chunk_record)
                    # session.commit()

                # Write chunks to local file
                write_chunks_details_to_file(all_chunk_details, chunks_output_path)
                logger.info("Chunks file saved to local")

                # Write chunks to S3 bucket
                # for chunk_file in os.listdir(self.chunks_output_path):
                #     if chunk_file.endswith(".json"):
                #         self.s3_io_util.upload_file(
                #             file_path=os.path.join(self.bioc_local_path, chunk_file),
                #             object_name=f"bioc_full_text_articles/{self.chunks_output_path}",
                #         )
                #         logger.info(f"Chunk file saved to S3")

    def get_chunks_embeddings_details(self, chunks: List[Dict], chunk_file_path: str):
        try:
            logger.info("Generating embeddings for the chunks")
            model_info = get_model_info(self.embeddings_model)
            merged_texts_with_sum = [f"{chunk['merged_text']}" for chunk in chunks]
            embeddings = get_embeddings(
                model_name=model_info[0],
                token_limit=model_info[1],
                texts=merged_texts_with_sum,
            )
            embeddings_details = {
                "file": chunk_file_path,
                "chunks_count": len(merged_texts_with_sum),
                "chunker_type": self.chunker,
                "merger_type": self.merger,
                "aioner_model": self.aioner_model,
                "gnorm2_model": self.gnorm2_model,
                "embeddings_model": self.embeddings_model,
                "embeddings_model_token_limit": model_info[1],
                "contains_summary": True,
                "embeddings": embeddings,
            }
            # print(f"Embedding details in get_embeddings(): {embeddings_details}")
            return embeddings_details
        except Exception as e:
            logger.error(f"Error while processing chunk: {e}")
            raise e

    def store_embeddings_details_at_local(
        self, embeddings_details, embeddings_filename: str, embeddings_output_dir: str
    ):
        # Write the Embeddings to a file:
        logger.info(f"Saving embeddings to local file: {embeddings_filename}")
        embeddings_file_path = f"{embeddings_output_dir}/{embeddings_filename}"
        save_embeddings_details_to_json(
            embeddings_details_list=[embeddings_details], filename=embeddings_file_path
        )

    def store_embeddings_details_at_vectordb(
        self, collection_type: str, chunk_file_path: str
    ):
        # Get the Qdrant Handler with for specific vector db config
        qdrant_handler = QdrantHandler(
            collection_type=collection_type, params=vectordb_config
        )
        qdrant_manager = qdrant_handler.get_qdrant_manager()

        # batch_size = 10  # Adjust batch size based on performance
        # batch = []

        # ToDo: Implement logic to load the chunks from S3

        logger.info("Putting the embeddings in QdrantDB")
        # Load the chunks file from local:
        with open(f"{chunk_file_path}", "r") as f:
            chunks = json.load(f)
            for chunk in chunks:
                model_info = get_model_info(self.embeddings_model)
                # logger.info("Generating embeddings for the chunk")
                chunk_embeddings = get_embeddings(
                    model_name=model_info[0],
                    token_limit=model_info[1],
                    # # For Baseline Chunks Processing
                    # texts=[chunk["payload"]["chunk_text"]],
                    # For Processed Chunks Processing
                    texts=[chunk["merged_text"]],
                )[0]
                # logger.info("Embedding generated!")
                chunk_payload = chunk["payload"]

                # # Add the Embeddings and the Payload to a batch
                # batch.append((chunk_embeddings, chunk_payload))

                # Insert into Qdrant
                qdrant_manager.insert_vector(
                    vector=chunk_embeddings, payload=chunk_payload
                )

            #     # Insert in batches
            #     if len(batch) >= batch_size:
            #         qdrant_manager.insert_vectors(batch)
            #         batch = []
            #
            # # Insert the remaining batch
            # if batch:
            #     qdrant_manager.insert_vectors(batch)

    def process_embeddings(
        self,
        embeddings_output_dir: str,
        collection_type: str,
        store_embeddings_locally: bool = True,
    ):
        # ToDo: Implement logic to load the chunks from S3
        # Load the chunks file from local:
        for chunks_file in os.listdir(self.chunks_output_dir):
            if chunks_file.endswith(".json"):
                logger.info(f"Processing chunks file {chunks_file}...")
                chunk_file_path = f"{chunks_output_dir}/{chunks_file}"
                with open(f"{chunk_file_path}", "r") as f:
                    chunks = json.load(f)
                    if store_embeddings_locally:
                        embeddings_details = self.get_chunks_embeddings_details(
                            chunks=chunks, chunk_file_path=chunk_file_path
                        )
                        # print(f"Embedding details in process_embeddings(): {embeddings_details}")
                        self.store_embeddings_details_at_local(
                            embeddings_details=embeddings_details,
                            embeddings_filename=f"{chunks_file.split('.')[0]}_embeddings.json",
                            embeddings_output_dir=embeddings_output_dir,
                        )
                    else:
                        self.store_embeddings_details_at_vectordb(
                            collection_type=collection_type,
                            chunk_file_path=chunk_file_path,
                        )

    def process(
        self,
        embeddings_output_dir: str,
        collection_type: str,
        store_embeddings_locally: bool = True,
    ):
        # # Create Chunks of the Gnorm2 Annotated Articles and store them while storing logs in PostgreSQL DB
        logger.info("Creating Chunks...")
        self.process_chunks()
        logger.info("Chunks created successfully!")

        logger.info("Creating and storing embeddings...")
        # Create Embeddings and store them locally or in vectorDB
        self.process_embeddings(
            embeddings_output_dir=embeddings_output_dir,
            collection_type=collection_type,
            store_embeddings_locally=store_embeddings_locally,
        )
        logger.info("Embeddings stored successfully")


if __name__ == "__main__":
    # Processed Chunks Paths
    chunks_output_dir = f"../../data/indexing/chunks"
    collection_type = "processed_pubmedbert"

    # # Baseline Chunks Paths
    # chunks_output_dir = f"../../data/indexing/baseline_chunks"
    # collection_type = "baseline"

    # Other Params
    articles_input_dir = f"../../data/ner_processed/gnorm2_annotated"
    articles_summary_dir = f"../../data/articles_metadata/summary"
    embeddings_output_dir = f"../../data/indexing/embeddings"
    embeddings_model = "pubmedbert"
    chunker = "sliding_window"
    merger = "prepend"

    article_processor = ArticleProcessor(
        embeddings_model=embeddings_model,
        chunker=chunker,
        merger=merger,
        articles_input_dir=articles_input_dir,
        articles_summary_dir=articles_summary_dir,
        chunks_output_dir=chunks_output_dir,
    )

    article_processor.process(
        embeddings_output_dir=embeddings_output_dir,
        collection_type=collection_type,
        store_embeddings_locally=False,
    )

    # article_processor.process_chunks()

    # article_processor.process_embeddings(store_embeddings_locally=False, embeddings_output_dir=embeddings_output_dir)
