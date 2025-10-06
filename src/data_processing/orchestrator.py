import math
import argparse
import multiprocessing
import uuid
from datetime import datetime
from typing import Dict, List
from collections import Counter
from transformers import AutoTokenizer

from src.data_processing.xml_to_html_conversion.xml_to_html_converter import (
    XmlToHtmlConverter,
)
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory

from src.data_processing.chunking.chunks_handler import (
    chunk_annotated_articles,
)
from src.data_processing.merging.merge_handler import merge_annotations
from src.pubtator_utils.embeddings_handler.embeddings_generator import (
    get_embeddings,
    get_model_info,
    save_embeddings_details_to_json,
)
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class ArticleProcessor:
    def __init__(
        self,
        workflow_id: str,
        source: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        write_to_s3: bool,
        s3_file_handler: FileHandler,
        s3_paths_config: dict[str, str],
        aioner_model: str = "Bioformer",
        gnorm2_model: str = "Bioformer",
        embeddings_model: str = "pubmedbert",
        chunker: str = "sliding_window",
        merger: str = "prepend",
        pubmedbert_model=None,
        pubmedbert_tokenizer=None,
        chemberta_model=None,
        chemberta_tokenizer=None,
    ):
        self.aioner_model = aioner_model
        self.gnorm2_model = gnorm2_model
        self.embeddings_model = embeddings_model
        self.pubmedbert_model = pubmedbert_model
        self.pubmedbert_tokenizer = pubmedbert_tokenizer
        self.chemberta_model = chemberta_model
        self.chemberta_tokenizer = chemberta_tokenizer
        self.chunker = chunker
        self.merger = merger
        self.source = source
        self.workflow_id = workflow_id
        self.articles_input_dir = (
            paths_config["annotations_merged_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.articles_summary_dir = (
            paths_config["summary_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.chunks_output_dir = (
            paths_config["chunks_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.embeddings_output_dir = (
            paths_config["embeddings_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.articles_metadata_dir = (
            paths_config["metadata_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.file_handler = file_handler

        self.write_to_s3 = write_to_s3
        if self.write_to_s3:
            self.s3_file_handler = s3_file_handler
            self.s3_chunks_dir = s3_paths_config["chunks_path"].replace(
                "{source}", source
            )
            self.s3_embeddings_dir = s3_paths_config["embeddings_path"].replace(
                "{source}", source
            )
        else:
            self.s3_file_handler = self.s3_chunks_dir = self.s3_embeddings_dir = None

    def get_article_summary(self, article_file):
        # ToDo: Put the article summariser in data ingestion orchestrator
        article_file_name = f"{article_file.split('.')[0]}.txt"
        logger.info(f"Fetching article {article_file_name} summary")
        article_file_summary_path = self.file_handler.get_file_path(
            self.articles_summary_dir, article_file_name
        )
        article_summary = self.file_handler.read_file(article_file_summary_path)
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

            logger.info(f"{summary_tokens}: summary_tokens")
            # For Baseline Processing
            # window_size = 512
            window_size = 512 if window_size <= 50 else window_size
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

    def get_annotation_ids_per_bioconcept(
        self, chunk_annotations: List[Dict]
    ) -> Dict[str, List[str]]:
        """
        Gets the annotation_ids per bioconcept.

        Args:
            chunk_annotations (List[Dict]): List of annotation dictionaries.

        Returns:
            Dict[str, List[str]]: Dictionary with List of ids per bioconcept, including empty list for those not present.
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
        annotation_id_dic = {}
        for concept in bioconcepts.union({"Variant"}):
            annotation_id_dic[concept] = set()

        # Count annotations ids per bioconcept, classifying unknown types as "Variant"
        for annotation in chunk_annotations:
            annotation_type = annotation.get("type")
            if annotation_type not in bioconcepts:
                annotation_type = "Variant"
            if annotation["identifier"] is not None:
                annotation_id_dic[annotation_type].add(annotation["identifier"])

        for annotation_id_per_concept in annotation_id_dic:
            annotation_id_dic[annotation_id_per_concept] = list(
                annotation_id_dic[annotation_id_per_concept]
            )

        return annotation_id_dic

    def get_unique_keyword_annotations(
        self, chunk_annotations: List[Dict]
    ) -> List[str]:
        unique_keywords = set()
        for annotation in chunk_annotations:
            annotation_text = annotation.get("text").lower()
            unique_keywords.add(annotation_text)

        return list(unique_keywords)

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
                s3_chunks_output_path = (
                    self.s3_file_handler.get_file_path(
                        self.s3_chunks_dir, chunk_output_file_name
                    )
                    if self.write_to_s3
                    else None
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
                    keywords = self.get_unique_keyword_annotations(chunk_annotations)
                    annotation_ids_per_bioconcept = (
                        self.get_annotation_ids_per_bioconcept(chunk_annotations)
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
                            "chunk_processing_date": datetime.now().date().isoformat(),
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
                            "keywords": keywords,
                            "gene_ids": annotation_ids_per_bioconcept["Gene"],
                            "species_ids": annotation_ids_per_bioconcept["Species"],
                            "strain_ids": annotation_ids_per_bioconcept["Strain"],
                            "genus_ids": annotation_ids_per_bioconcept["Genus"],
                            "cell_line_ids": annotation_ids_per_bioconcept["CellLine"],
                            "disease_ids": annotation_ids_per_bioconcept["Disease"],
                            "chemical_ids": annotation_ids_per_bioconcept["Chemical"],
                            "variant_ids": annotation_ids_per_bioconcept["Variant"],
                            # "chunk_offset": chunk_offset,
                            # "chunk_infons": chunk_infons,
                            # "chunker_type": chunker_type,
                            # "merger_type": merger_type,
                            # "embeddings_model": embeddings_model,
                            # "aioner_model": aioner_model,
                            # "gnorm2_model": gnorm2_model,
                            "article_id": article_id,
                            "article_summary": article_summary,
                            "source": self.source,
                            "workflow_id": self.workflow_id,
                            "chunk_type": "article_chunk",
                            "processing_ts": datetime.now().isoformat(),
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

                if self.write_to_s3:
                    # Save Chunks to S3
                    self.s3_file_handler.write_file_as_json(
                        s3_chunks_output_path, all_chunk_details
                    )
                    logger.info(f"Chunks file saved to S3: {s3_chunks_output_path}")

    def get_chunks_embeddings_details(
        self,
        chunks: List[Dict],
        collection_type: str,
        model=None,
        tokenizer=None,
        tag_name: str = "merged_text",
    ):
        try:
            logger.info("Generating embeddings for the chunks")
            chunk_texts = []
            skip_embedding = None
            embeddings = None

            for chunk in chunks:
                chunk_texts.append(
                    # chunk["merged_text"]
                    chunk[tag_name]
                    if collection_type == "processed_pubmedbert"
                    else chunk["payload"]["chunk_text"]
                )
                skip_embedding = (
                    True
                    if tag_name == "smile"
                    and chunk[tag_name] is None
                    and skip_embedding != False
                    else False
                )

            if not skip_embedding:
                embeddings = get_embeddings(
                    model_name=self.embeddings_model,
                    texts=chunk_texts,
                    model=model,
                    tokenizer=tokenizer,
                )

            chunk_embedding_payload = []
            for idx, chunk in enumerate(chunks):
                cur_chunk_dic = {}
                chunk_payload = chunk["payload"]
                chunk_payload[tag_name] = chunk[tag_name]
                cur_chunk_dic["payload"] = chunk_payload
                if not skip_embedding:
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
        s3_embeddings_file_path = (
            self.s3_file_handler.get_file_path(
                self.s3_embeddings_dir, embeddings_filename
            )
            if self.write_to_s3
            else None
        )
        logger.info(f"Saving embeddings to file: {embeddings_file_path}")
        # embeddings_file_path = f"{self.embeddings_output_dir}/{embeddings_filename}"
        save_embeddings_details_to_json(
            embeddings_details_list=embeddings_details,
            filename=embeddings_file_path,
            file_handler=self.file_handler,
            write_to_s3=self.write_to_s3,
            s3_filename=s3_embeddings_file_path,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info(f"Saved embeddings to file S3: {s3_embeddings_file_path}")

    def process_embeddings(
        self,
        # embeddings_output_dir: str,
        collection_type: str,
        store_embeddings_as_file=True,
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
                    if "smile" in chunks[0].keys():
                        self.embeddings_model = "chemberta"
                        embeddings_details = self.get_chunks_embeddings_details(
                            chunks=chunks,
                            collection_type=collection_type,
                            model=self.chemberta_model,
                            tokenizer=self.chemberta_tokenizer,
                            tag_name="smile",
                        )
                    else:
                        self.embeddings_model = "pubmedbert"
                        embeddings_details = self.get_chunks_embeddings_details(
                            chunks=chunks,
                            collection_type=collection_type,
                            model=self.pubmedbert_model,
                            tokenizer=self.pubmedbert_tokenizer,
                        )
                    # print(f"Embedding details in process_embeddings(): {embeddings_details}")
                    self.store_embeddings_details_in_file(
                        embeddings_details=embeddings_details,
                        embeddings_filename=f"{chunks_file.split('.')[0]}_embeddings.json",
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
        # Create Embeddings and store them locally
        self.process_embeddings(
            # embeddings_output_dir=self.embeddings_output_dir,
            collection_type=collection_type,
            store_embeddings_as_file=store_embeddings_as_file,
        )
        logger.info("Embeddings stored successfully")


def run_chunking_and_embedding(
    workflow_id: str,
    source: str,
    write_to_s3: bool = True,
    run_type: str = "all",
    pubmedbert_model=None,
    pubmedbert_tokenizer=None,
    chemberta_model=None,
    chemberta_tokenizer=None,
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

    s3_paths = {}
    s3_file_handler = None
    if write_to_s3:
        # Get S3 Paths and file handler for writing to S3
        storage_type = "s3"
        s3_paths = paths_config["storage"][storage_type]
        s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    article_processor = ArticleProcessor(
        workflow_id=workflow_id,
        source=source,
        embeddings_model=embeddings_model,
        chunker=chunker,
        merger=merger,
        file_handler=file_handler,
        paths_config=paths,
        write_to_s3=write_to_s3,
        s3_file_handler=s3_file_handler,
        s3_paths_config=s3_paths,
        pubmedbert_model=pubmedbert_model,
        pubmedbert_tokenizer=pubmedbert_tokenizer,
        chemberta_model=chemberta_model,
        chemberta_tokenizer=chemberta_tokenizer,
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
        logger.error(f"Invalid run type: {run_type}")


def run_xml_to_html_conversion(workflow_id: str, source: str, write_to_s3: bool = True):
    """
    Run XML to HTML conversion for the given workflow ID and source.
    This function converts Annotated BioCXML files to HTML files using a specified template to generated Pubtator-Style Annotated Articles View.
    :param workflow_id:
    :param source:
    :param write_to_s3:
    :return:
    """
    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    xml_to_html_template_path = paths_config["xml_to_html_template_path"]
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    s3_paths = {}
    s3_file_handler = None
    if write_to_s3:
        # Get S3 Paths and file handler for writing to S3
        storage_type = "s3"
        s3_paths = paths_config["storage"][storage_type]
        s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    html_converter = XmlToHtmlConverter(
        workflow_id,
        source,
        paths,
        file_handler,
        xml_to_html_template_path,
        write_to_s3,
        s3_paths,
        s3_file_handler,
    )
    html_converter.xml_html_converter()


def _load_embeddings_models(model_name: str = "pubmedbert"):
    try:
        from src.pubtator_utils.embeddings_handler.embeddings_generator import (
            load_embeddings_model,
        )

        model, tokenizer = load_embeddings_model(model_name=model_name)
        logger.info("Embeddings Model and Tokenizer loaded successfully at startup.")
        return model, tokenizer
    except Exception as e:
        logger.warn(f"Failed to load embeddings model due to {e}.")
        return None, None


def _safe_run_chunking_and_embedding(
    workflow_id,
    source,
    write_to_s3,
    run_type,
    collection_type,
    store_embeddings_as_file,
):
    # Pre-load the embeddings model and tokenizer at startup
    try:
        pubmedbert_model, pubmedbert_tokenizer = _load_embeddings_models(
            model_name="pubmedbert"
        )
        chemberta_model, chemberta_tokenizer = _load_embeddings_models(
            model_name="chemberta"
        )
    except Exception as e:
        logger.warn(
            f"Failed to load embeddings model due to {e}. It will be loaded at runtime."
        )
        pubmedbert_model, pubmedbert_tokenizer = None, None
        chemberta_model, chemberta_tokenizer = None, None

    try:
        run_chunking_and_embedding(
            workflow_id=workflow_id,
            run_type=run_type,
            source=source,
            write_to_s3=write_to_s3,
            pubmedbert_model=pubmedbert_model,
            pubmedbert_tokenizer=pubmedbert_tokenizer,
            chemberta_model=chemberta_model,
            chemberta_tokenizer=chemberta_tokenizer,
            collection_type=collection_type,
            store_embeddings_as_file=store_embeddings_as_file,
        )
        logger.info("Chunking & embedding finished successfully.")
    except Exception:
        logger.exception("Chunking & embedding failed")


def _safe_run_xml_to_html_conversion(
    workflow_id: str, source: str, write_to_s3: bool = True
):
    try:
        run_xml_to_html_conversion(
            workflow_id=workflow_id, source=source, write_to_s3=write_to_s3
        )
        logger.info("XML→HTML conversion finished successfully.")
    except Exception:
        logger.exception("XML→HTML conversion failed")


def main():
    """
    Main function to run the PMC Ingestor with improved command-line interface.
    """
    logger.info("Execution Started for Processing pipeline")

    parser = argparse.ArgumentParser(
        description="Ingest articles",
        epilog="Example: python3 -m src.data_processing.orchestrator --workflow_id workflow123 --source ct",
    )

    parser.add_argument(
        "--workflow_id",
        "-wid",
        type=str,
        required=True,
        help="Workflow ID of JIT pipeline run",
    )

    parser.add_argument(
        "--source",
        "-src",
        type=str,
        required=True,
        choices=["pmc", "ct", "preprint", "rfd", "eln", "apollo", "ss"],
        help="Article source (allowed values: pmc, ct, preprint, rfd, eln, apollo, ss)",
    )

    parser.add_argument(
        "--write_to_s3",
        "-s3",
        type=str,
        default=True,
        help="Whether to write ingested data to S3 (default: True)",
    )

    parser.add_argument(
        "--collection_type",
        type=str,
        default="processed_pubmedbert",
        help="OpenSearch Collection Type",
    )

    parser.add_argument(
        "--run_type",
        type=str,
        default="all",
        help="Select run type out of ['all', 'chunks', 'embeddings']",
    )

    parser.add_argument(
        "--store_embeddings_as_file",
        type=bool,
        default=True,
        help="Select if embeddings should be stored as file",
    )

    args = parser.parse_args()

    if not args.workflow_id:
        logger.error("No workflow_id provided.")
        return
    else:
        workflow_id = args.workflow_id
        logger.info(f"{workflow_id} Workflow Id registered for processing")

    if not args.source:
        logger.error("No source provided. Please provide a valid source.")
        return
    else:
        source = args.source
        logger.info(f"{source} registered as SOURCE for processing")

    if not args.write_to_s3:
        logger.warning("No write_to_s3 flag provided. Defaulting to True.")
        write_to_s3 = True
    else:
        write_to_s3 = (
            True if args.write_to_s3.lower() in ("true", "1", "yes") else False
        )
        logger.info(f"write_to_s3 set to {write_to_s3}")

    if not args.collection_type:
        logger.info(
            "No collection_type provided. Using default: `processed_pubmedbert` "
            "Out of ['baseline', 'test', 'processed_pubmedbert']"
        )
        collection_type = "processed_pubmedbert"
    else:
        collection_type = args.collection_type
        logger.info(f"Collection Type: {collection_type}")

    if not args.run_type:
        logger.info(
            "No run_type provided. Using default: `all` out of ['all', 'chunks', 'embeddings']"
        )
        run_type = "all"
    else:
        run_type = args.run_type
        logger.info(f"Run Type: {run_type}")

    if not args.store_embeddings_as_file:
        logger.info("No store_embeddings_as_file provided. Using default: `True`")
        store_embeddings_as_file = True
    else:
        store_embeddings_as_file = args.store_embeddings_as_file
        logger.info(f"Store Embeddings As File: {store_embeddings_as_file}")

    logger.info(
        f"Execution Started for Processing pipeline for workflow_id: {workflow_id}"
    )

    # set up two separate processes
    p1 = multiprocessing.Process(
        target=_safe_run_chunking_and_embedding,
        args=(
            workflow_id,
            source,
            write_to_s3,
            run_type,
            collection_type,
            store_embeddings_as_file,
        ),
    )
    p2 = multiprocessing.Process(
        target=_safe_run_xml_to_html_conversion,
        args=(
            workflow_id,
            source,
            write_to_s3,
        ),
    )

    # start both
    p1.start()
    p2.start()

    # wait for both to finish
    p1.join()
    p2.join()

    logger.info("Execution Completed for Processing pipeline!")


if __name__ == "__main__":
    main()
