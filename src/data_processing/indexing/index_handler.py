import json
import os

from biorun.convert import false

from src.vector_db_handler.index_factory import IndexFactory
from src.utils.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def process_chunked_data(vector_db, chunk, embed_model):
    """Method to process each chunk."""

    chunk_payload = chunk["payload"]
    chunker_type = chunk_payload["chunker_type"]
    merger_type = chunk_payload["merger_type"]
    ner_model = chunk_payload["aioner_model"]

    # Get the appropriate indexer instance
    index_factory = IndexFactory(chunker_type, merger_type, ner_model, embed_model)
    index_handler = index_factory.get_indexer(vector_db)

    if vector_db == "qdrant":
        index_handler.insert_chunk(chunk)
    else:
        raise ValueError(f"Unknown Vector DB type: {vector_db}")


def process_chunked_data_file(file_path, vector_db, embed_model):
    """Method to process the chunks file."""

    # Open and read the JSON file
    with open(file_path, "r") as file:
        chunks_data = json.load(file)
        for chunk in chunks_data:
            process_chunked_data(vector_db, chunk, embed_model)


# Run the main function
if __name__ == "__main__":
    vector_db_list = ["qdrant"]
    llm_model = "BedrockClaude"

    chunk_strategies = [
        # "grouped_annotation_aware_sliding_window_prepend_bioformer",
        # "grouped_annotation_aware_sliding_window_inline_bioformer",
        "grouped_annotation_aware_sliding_window_append_bioformer",
        # "grouped_annotation_aware_sliding_window_prepend_pubmedbert",
        # "grouped_annotation_aware_sliding_window_inline_pubmedbert",
        # "grouped_annotation_aware_sliding_window_append_pubmedbert",
    ]

    # chunk_strategy = 'grouped_annotation_aware_sliding_window_append_bioformer'
    # chunk_strategy = "grouped_annotation_aware_sliding_window_inline_bioformer"
    # chunk_strategy = 'grouped_annotation_aware_sliding_window_append_pubmedbert'
    # chunk_strategy = 'grouped_annotation_aware_sliding_window_inline_pubmedbert'

    # chunk_strategy = 'sliding_window_append_bioformer'
    # chunk_strategy = 'sliding_window_inline_bioformer'
    # chunk_strategy = 'sliding_window_append_pubmedbert'
    # chunk_strategy = 'sliding_window_inline_pubmedbert'

    articles_to_process = [
        "PMC_7614604",
        # "PMC_9911803"
    ]

    for chunk_strategy in chunk_strategies:
        cnt = 0
        print(chunk_strategy)
        for cur_vector_db in vector_db_list:
            for cur_file in os.listdir("../../../data/chunks_11_oct"):
                if (
                    cur_file.endswith(".json")
                    and cur_file.startswith(chunk_strategy)
                    and articles_to_process[0] in cur_file
                ):
                    cnt += 1
                    print(f"cur_file: {cur_file}, cnt: {cnt}")
                    logger.info(f"Processing {cur_file}")
                    input_file_path = f"../../../data/chunks_11_oct/{cur_file}"
                    process_chunked_data_file(input_file_path, cur_vector_db, llm_model)
                    logger.info(f"Finished processing {cur_file}")
        print(cnt)
