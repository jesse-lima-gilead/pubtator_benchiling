import json
import os
import uuid
from typing import Dict, List

from src.utils.db import session  # Import the session
from src.alembic_models.chunks import Chunk  # Import the Chunk model
from src.utils.s3_io_util import S3IOUtil
from src.data_processing.chunking.chunks_handler import chunk_annotated_articles, write_chunks_to_file
from src.vector_db_handler.qdrant_handler import QdrantHandler
from src.data_processing.merging.merge_handler import merge_annotations
from src.utils.articles_summarizer import SummarizeArticle
from src.data_processing.embedding.embeddings_handler import (
    get_embeddings,
    get_model_info,
    save_embeddings_details_to_json
)
from src.utils.logger import SingletonLogger
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
            aioner_model: str,
            gnorm2_model: str,
            input_article_path: str,
            embeddings_model: str,
            chunker: str,
            merger: str,
            chunks_output_path: str,
    ):
        self.aioner_model = aioner_model
        self.gnorm2_model = gnorm2_model
        self.input_article_path = input_article_path
        self.embeddings_model = embeddings_model
        self.chunker = chunker
        self.merger = merger
        self.chunks_output_path = chunks_output_path
        self.article_id = os.path.splitext(os.path.basename(self.input_article_path))[0]
        self.qdrant_manager = QdrantHandler.get_qdrant_manager()

    def get_article_chunks(self):
        chunks = chunk_annotated_articles(
            input_file_path=self.input_articles_dir,
            chunker_type=self.chunker,
            output_path=self.chunks_output_path,
        )
        return chunks

    def get_chunks_with_merged_annotations(self):
        chunks = self.get_article_chunks()
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

    def get_chunks_with_summary(self):
        summarizer = SummarizeArticle(input_file_path=self.input_article_path)
        summary = summarizer.summarize().content

        chunks = self.get_chunks_with_merged_annotations()
        chunks_with_summary = []

        for i, chunk in enumerate(chunks):
            chunk["summary"] = summary
            chunk["merged_text_with_summary"] = f"Summary:\n{summary}\nText:\n{chunk['merged_text']}"
            chunks_with_summary.append(chunk)

        return chunks_with_summary

    def process_chunks(self):
        chunks = self.get_chunks_with_summary()
        all_chunk_details = []

        for i, chunk in enumerate(chunks):
            chunk_id = str(uuid.uuid4())
            chunk_sequence = f"{i + 1}"
            chunk_name = f"{self.article_id}_chunk_{chunk_sequence}"
            article_summary = chunk["summary"]
            article_id = self.article_id
            chunk_text = chunk["text"]
            merged_text = chunk["merged_text"]
            merged_text_with_summary = chunk["merged_text_with_summary"]
            chunk_annotations = chunk["annotations"]
            chunk_length = len(chunk_text)
            token_count = len(chunk_text.split())
            chunk_annotations_count = len(chunk_annotations)
            chunk_annotations_ids = [ann["id"] for ann in chunk_annotations]
            chunk_annotations_types = list(set([ann["type"] for ann in chunk_annotations]))
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
            session.add(chunk_record)
            session.commit()

        # Write chunks to local file
        write_chunks_to_file(chunks, self.chunks_output_path)

        # Write chunks to S3 bucket
        # for chunk_file in os.listdir(self.chunks_output_path):
        #     if chunk_file.endswith(".json"):
        #         self.s3_io_util.upload_file(
        #             file_path=os.path.join(self.bioc_local_path, chunk_file),
        #             object_name=f"bioc_full_text_articles/{self.chunks_output_path}",
        #         )
        #         logger.info(f"Chunk file saved to S3")


    def get_chunks_embeddings_details(self, chunks: List[Dict]):
        try:
            model_info = get_model_info(self.embeddings_model)
            merged_texts_with_sum = [f"{chunk['merged_text']}" for chunk in chunks]
            embeddings = get_embeddings(
                model_name=model_info[0],
                token_limit=model_info[1],
                texts=merged_texts_with_sum,
            )
            embeddings_details = {
                "file": self.chunks_output_path,
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
            return embeddings_details
        except Exception as e:
            logger.error(f"Error while processing chunk: {e}")
            raise e

    def store_embeddings_details_at_local(self):

        # ToDo: Implement logic to load the chunks from S3

        # Load the chunks file from local:
        with open(f"{self.chunks_output_path}", "r") as f:
            chunks = json.load(f)
            embeddings_details = self.get_chunks_embeddings_details(chunks)

        # Write the Embeddings to a file:
        file_path = f"../../data/decoy_docs/embeddings/_embeddings.json"
        save_embeddings_details_to_json(embeddings_details, file_path)

    def store_embeddings_details_at_vectordb(self):
        # ToDo: Implement logic to load the chunks from S3
        all_embedding_details = []

        # Load the chunks file from local:
        with open(f"{self.chunks_output_path}", "r") as f:
            chunks = json.load(f)
            for chunk in chunks:
                chunk_embeddings = self.get_chunk_embeddings_details(chunk)['embeddings']
                chunk_payload = chunk['payload']
                # Insert into Qdrant
                self.qdrant_manager.insert_vector(chunk_embeddings, chunk_payload)


if __name__ == "__main__":
    annotation_models_list = ["bioformer", "pubmedbert"]

    chunker_list = [
        "sliding_window",
        "passage",
        "annotation_aware",
        "grouped_annotation_aware_sliding_window",
    ]

    merger_list = [
        "append",
        "inline",
        "prepend",
        # "fulltext"
    ]

    input_articles_dir = f"../../../data/gnorm2_annotated/{annotation_models_list[0]}_annotated"

    article_processor = ArticleProcessor(
        input_articles_dir=input_articles_dir,
        embedding_model="pubmedbert",
        chunker="sliding_window",
        merger="prepend",
        chunks_output_path="../data/chunks"
    )

    article_processor.process()



# [
#     [0.23, 0.45, 0.67],
#     [0.45, 0.67, 0.89],
#     [0.72, 0.34, 0.56]
# ]
#
# [
# [{(article1_chunk_id, article2_chunk_id): score}, {(article1_chunk_id, article2_chunk_id): score}, {(article1_chunk_id, article2_chunk_id): score}]
# [{(article1_chunk_id, article2_chunk_id): score}, {(article1_chunk_id, article2_chunk_id): score}, {(article1_chunk_id, article2_chunk_id): score}]
# [{(article1_chunk_id, article2_chunk_id): score}, {(article1_chunk_id, article2_chunk_id): score}, {(article1_chunk_id, article2_chunk_id): score}]
# ]
#
#
# # Vector DB Collection Points
#
# # Insert into Qdrant
# self.qdrant_manager.insert_vector(embedding, payload)
#
chunk_payload = {
    "chunk_id": chunk_id,
    "chunk_name": chunk_name,
    "token_count": token_count,
    "chunk_annotations_count": chunk_annotations_count,
    "chunker_type": chunker_type,
    "merger_type": merger_type,
    "embeddings_model": embeddings_model,
    "aioner_model": aioner_model,
    "gnorm2_model": gnorm2_model,
    "article_id": article_id,
    "article_summary": article_summary,
    "chunk_sequence": chunk_sequence,
    "chunk_text": chunk_text,
    "chunk_annotations": chunk_annotations,
}
#
# # Insert into PostgreSQL
# chunk_record = Chunk(
#     chunk_id=chunk_id,
#     chunk_name=chunk_name,
#     file_name=file_name,
#     file_path=pdf_path,
#     chunk_index=chunk_index + 1,
#     chunk_size=len(chunk),
# "chunk_length": chunk_length,
# "chunk_annotations_ids": chunk_annotations_ids,
# "chunk_annotations_types": chunk_annotations_types,
# "chunk_offset": chunk_offset,
# "chunk_infons": chunk_infons,
#     text=chunk,
# )
# session.add(chunk_record)
# session.commit()