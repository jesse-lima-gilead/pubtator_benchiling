# ToDo: Add the orchestration logic here

from src.data_processing.chunking.chunker_factory import ChunkerFactory


# Example usage of the factory method
def main():
    xml_file = "../../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/PMC_8005792.xml"
    factory = ChunkerFactory(xml_file, max_tokens_per_chunk=512)

    # Select the chunker type (e.g., 'passage' or 'annotation_aware')
    chunker_type = (
        "annotation_aware"  # 'sliding_window' or 'passage' or 'annotation_aware'
    )

    # Get the appropriate chunker instance
    chunker = factory.get_chunker(chunker_type)

    # Perform chunking using the selected chunker
    if chunker_type == "passage":
        chunks = chunker.passage_based_chunking()
    elif chunker_type == "annotation_aware":
        chunks = chunker.annotation_aware_chunking()
    elif chunker_type == "sliding_window":
        chunks = chunker.sliding_window_chunking()

    # Print the chunks for verification
    for i, chunk in enumerate(chunks):
        print(f"Chunk {i + 1}: {chunk}")


# Run the main function
if __name__ == "__main__":
    main()
