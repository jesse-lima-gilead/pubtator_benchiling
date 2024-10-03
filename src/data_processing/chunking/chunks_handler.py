# ToDo: Add the orchestration logic here

from src.data_processing.chunking.chunker_factory import ChunkerFactory


# Example usage of the factory method
def main():
    xml_file = "../../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/PMC_8005792.xml"
    factory = ChunkerFactory(xml_file, max_tokens_per_chunk=512)

    # Select the chunker type
    # (e.g., 'sliding_window' or 'passage' or 'annotation_aware' or 'grouped_annotation_aware_sliding_window')
    chunker_type = "grouped_annotation_aware_sliding_window"  # Change this to test different chunkers

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


# Run the main function
if __name__ == "__main__":
    main()
