import os
import json
from src.data_processing.chunking.chunker_factory import ChunkerFactory


def write_chunks_to_file(chunks, output_file: str):
    """Write the chunks to a JSON file."""
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=4)


# Example usage of the factory method
def chunk_annotated_articles(
    input_file_path: str,
    chunker_type: str,
):
    # xml_file = "../../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/PMC_8005792.xml"
    # article_id = os.path.splitext(os.path.basename(input_file_path))[0]

    # Get the appropriate chunker instance
    chunker_factory = ChunkerFactory(input_file_path, max_tokens_per_chunk=512)
    chunker = chunker_factory.get_chunker(chunker_type)

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

    return chunks

    # # Get the appropriate Text - Annotations Merger
    # merger_factory = TextAnnotationMergeFactory(
    #     input_file_path, max_tokens_per_chunk=512
    # )
    # merger = merger_factory.get_merger(merger_type)
    #
    # # Print the chunks for verification
    # for i, chunk in enumerate(chunks):
    #     print(f"Chunk {i + 1}: {chunk}")
    #
    # # Getting the Chunks details:
    #
    # all_chunk_details = []
    #
    # for i, chunk in enumerate(chunks):
    #     merged_text = merger.merge(chunk["text"], chunk["annotations"])
    #
    #     chunk_id = str(uuid.uuid4())
    #     chunk_sequence = f"{i + 1}"
    #     chunk_name = f"{article_id}_chunk_{chunk_sequence}"
    #     chunk_text = chunk["text"]
    #     chunk_annotations = chunk["annotations"]
    #     chunk_length = len(chunk_text)
    #     token_count = len(chunk_text.split())
    #     chunk_annotations_count = len(chunk_annotations)
    #     chunk_annotations_ids = [ann["id"] for ann in chunk_annotations]
    #     chunk_annotations_types = list(set([ann["type"] for ann in chunk_annotations]))
    #     chunk_offset = chunk["offset"]
    #     chunk_infons = chunk["infons"]
    #     chunker_type = chunker_type
    #
    #     chunk_details = {
    #         "chunk_sequence": chunk_sequence,
    #         "merged_text": merged_text,
    #         "chunk_text": chunk_text,
    #         "chunk_annotations": chunk_annotations,
    #         "payload": {
    #             "chunk_id": chunk_id,
    #             "chunk_name": chunk_name,
    #             "chunk_length": chunk_length,
    #             "token_count": token_count,
    #             "chunk_annotations_count": chunk_annotations_count,
    #             "chunk_annotations_ids": chunk_annotations_ids,
    #             "chunk_annotations_types": chunk_annotations_types,
    #             "chunk_offset": chunk_offset,
    #             "chunk_infons": chunk_infons,
    #             "chunker_type": chunker_type,
    #             "merger_type": merger_type,
    #             "aioner_model": aioner_model,
    #             "gnorm2_model": gnorm2_model,
    #             "article_id": article_id,
    #             # "article_summary": article_summary,
    #         },
    #     }
    #     print(chunk_details)
    #     all_chunk_details.append(chunk_details)
    #
    #     # Insert into PostgreSQL
    #     chunk_record = Chunk(
    #         chunk_id=chunk_id,
    #         chunk_sequence=chunk_sequence,
    #         chunk_name=chunk_name,
    #         chunk_length=chunk_length,
    #         token_count=token_count,
    #         chunk_annotations_count=chunk_annotations_count,
    #         chunk_annotations_ids=chunk_annotations_ids,
    #         chunk_annotations_types=chunk_annotations_types,
    #         chunk_offset=chunk_offset,
    #         chunk_infons=chunk_infons,
    #         chunker_type=chunker_type,
    #         merger_type=merger_type,
    #         aioner_model=aioner_model,
    #         gnorm2_model=gnorm2_model,
    #         article_id=article_id,
    #     )
    #     session.add(chunk_record)
    #     session.commit()
    #
    # # Write Chunks to file:
    # write_chunks_to_file(
    #     all_chunk_details,
    #     output_file=f"{output_path}/{chunker_type}_{merger_type}_{gnorm2_model}_{article_id}.json",
    # )


# Run the main function
if __name__ == "__main__":
    models_list = ["bioformer", "pubmedbert"]

    chunker_list = [
        "sliding_window",
        # "passage",
        # "annotation_aware",
        # "grouped_annotation_aware_sliding_window",
    ]

    merger_list = [
        # "append",
        # "inline",
        "prepend",
        # "fulltext"
    ]

    output_path = "../../../data/chunks_11_oct"

    # # Single File Run for DEMO:
    # # Select the chunker type
    # # (e.g., 'sliding_window' or 'passage' or 'annotation_aware' or 'grouped_annotation_aware_sliding_window')
    # chunker = "grouped_annotation_aware_sliding_window"  # Change this to test different chunkers
    # merger_type = "inline"
    # input_file_path = "../../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/PMC_8005792.xml"
    #
    # chunk_annotated_articles(
    #     chunker_type=chunker,
    #     merger_type=merger_type,
    #     input_file_path=input_file_path,
    #     output_path=output_path,
    #     aioner_model="bioformer",
    #     gnorm2_model="bioformer",
    # )

    # Multiple Files Run:
    for model in models_list:
        for chunker in chunker_list:
            for merger in merger_list:
                for file in os.listdir(
                    f"../../../data/gnorm2_annotated/{model}_annotated"
                ):
                    if file.endswith(".xml"):
                        input_file_path = (
                            f"../../../data/gnorm2_annotated/{model}_annotated/{file}"
                        )

                        # Summarise the file content
                        # article_summary = summarize_article(input_file_path)

                        # Creating the chunks
                        chunk_annotated_articles(
                            # article_summary=article_summary,
                            chunker_type=chunker,
                            merger_type=merger,
                            input_file_path=input_file_path,
                            output_path=output_path,
                            aioner_model=model,
                            gnorm2_model=model,
                        )
