import os
import json
import uuid
from typing import List, Dict, Any
from src.utils.db import session  # Import the session
from src.alembic_models.chunks import Chunk  # Import the Chunk model
from src.data_processing.chunking.chunker_factory import ChunkerFactory


def write_chunks_to_file(chunks, output_file: str):
    """Write the chunks to a JSON file."""
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=4)


def merge_text_and_annotations_with_offsets(chunk_text, chunk_annotations):
    """
    Merge chunk text with annotations, inserting annotation information based on their offsets.

    Args:
        chunk_text (str): The text of the chunk.
        chunk_annotations (list): A list of annotations, each containing offset, length, and text.

    Returns:
        str: The merged text with annotations inserted at the correct offsets.
    """
    # Sort annotations by their offset to handle the correct order of insertion
    chunk_annotations.sort(key=lambda x: x["offset"])

    merged_text = ""
    current_position = 0

    for annotation in chunk_annotations:
        ann_offset = annotation["offset"]
        ann_text = annotation["text"]
        ann_type = annotation["type"]
        ncbi_id = annotation["infons"].get("NCBI_id", "NA")

        # Append the text between the last position and the current annotation offset
        merged_text += chunk_text[current_position:ann_offset]

        # Insert the annotation information after the text it applies to
        annotation_insert = f"[{ann_type}: {ann_text} (NCBI:{ncbi_id})]"

        # Append the annotated text
        merged_text += chunk_text[ann_offset : ann_offset + annotation["length"]]

        # Append the annotation metadata
        merged_text += annotation_insert

        # Update the current position
        current_position = ann_offset + annotation["length"]

    # Append the remaining text after the last annotation
    merged_text += chunk_text[current_position:]

    return merged_text


# Example usage of the factory method
def chunk_annotated_articles(chunker_type: str, input_file_path: str, output_path: str):
    # xml_file = "../../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/PMC_8005792.xml"
    article_id = os.path.splitext(os.path.basename(input_file_path))[0]
    factory = ChunkerFactory(input_file_path, max_tokens_per_chunk=512)

    # Get the appropriate chunker instance
    chunker = factory.get_chunker(chunker_type)

    # Perform chunking using the selected chunker
    if chunker_type == "passage":
        chunks = chunker.passage_based_chunking()
    elif chunker_type == "annotation_aware":
        chunks = chunker.annotation_aware_chunking()
    elif chunker_type == "grouped_annotation_aware_sliding_window":
        chunks = chunker.grouped_annotation_aware_sliding_window_chunking()
    elif chunker_type == "sliding_window":
        chunks = chunker.sliding_window_chunking()
    else:
        raise ValueError(f"Unknown chunker type: {chunker_type}")

    # Print the chunks for verification
    for i, chunk in enumerate(chunks):
        print(f"Chunk {i + 1}: {chunk}")

    # Getting the Chunks details:

    all_chunk_details = []

    for i, chunk in enumerate(chunks):
        # merged_text = merge_text_and_annotations_with_offsets(chunk["text"], chunk["annotations"])

        chunk_id = str(uuid.uuid4())
        chunk_sequence = f"{i + 1}"
        chunk_name = f"{article_id}_chunk_{chunk_sequence}"
        chunk_text = chunk["text"].strip()
        chunk_annotations = chunk["annotations"]
        chunk_length = len(chunk_text)
        token_count = len(chunk_text.split())
        chunk_annotations_count = len(chunk_annotations)
        chunk_offset = chunk["offset"]
        chunk_infons = chunk["infons"]
        chunker_type = chunker_type

        chunk_details = {
            "chunk_sequence": chunk_sequence,
            "chunk_text": chunk_text,
            "chunk_annotations": chunk_annotations,
            "payload": {
                "chunk_id": chunk_id,
                "chunk_name": chunk_name,
                "chunk_length": chunk_length,
                "token_count": token_count,
                "chunk_annotations_count": chunk_annotations_count,
                "chunk_offset": chunk_offset,
                "chunk_infons": chunk_infons,
                "chunker_type": chunker_type,
                "article_id": article_id,
            },
        }
        # print(chunk_details)
        all_chunk_details.append(chunk_details)

        # Insert into PostgreSQL
        chunk_record = Chunk(
            chunk_id=chunk_id,
            chunk_sequence=chunk_sequence,
            chunk_name=chunk_name,
            chunk_length=chunk_length,
            token_count=token_count,
            chunk_annotations_count=chunk_annotations_count,
            chunk_offset=chunk_offset,
            chunk_infons=chunk_infons,
            chunker_type=chunker_type,
            article_id=article_id,
        )
    #     session.add(chunk_record)
    #     session.commit()
    #
    # # Write Chunks to file:
    # write_chunks_to_file(
    #     all_chunk_details, output_file=f"{output_path}/{chunker_type}_{article_id}.json"
    # )


# Run the main function
if __name__ == "__main__":
    chunker_list = [
        "sliding_window",
        # "passage",
        # "annotation_aware",
        # "grouped_annotation_aware_sliding_window",
    ]

    output_path = "../../../data/chunks"

    # Select the chunker type
    # (e.g., 'sliding_window' or 'passage' or 'annotation_aware' or 'grouped_annotation_aware_sliding_window')
    chunker = "grouped_annotation_aware_sliding_window"  # Change this to test different chunkers
    input_file_path = "../../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/PMC_8005792.xml"

    chunk_annotated_articles(
        chunker_type=chunker,
        input_file_path=input_file_path,
        output_path=output_path,
    )

    # for chunker in chunker_list:
    #     for file in os.listdir(
    #         "../../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated"
    #     ):
    #         if file.endswith(".xml"):
    #             input_file_path = f"../../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/{file}"
    #             chunk_annotated_articles(
    #                 chunker_type=chunker,
    #                 input_file_path=input_file_path,
    #                 output_path=output_path,
    #             )
