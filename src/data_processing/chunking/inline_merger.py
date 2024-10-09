class InlineMerger:
    def merge(self, text: str, annotations: list):
        """
        Merges annotations inline with the text based on their offset and length.

        Args:
        - chunk: a dictionary containing 'text' and 'annotations'

        Returns:
        - Merged text with annotations placed inline.
        """

        # Start with the original text
        merged_text = text

        # Create a set of unique annotations based on 'text', 'type', 'ncbi_label', and 'ncbi_id'
        # TODO: Bug - Sometimes some duplicate annotations are still getting picked up
        unique_annotations = {
            (ann["text"], ann["type"], ann["ncbi_label"], ann["ncbi_id"])
            for ann in annotations
        }

        # Replace text using replace() to avoid offset disruption
        for ann_text, ann_type, ncbi_label, ncbi_id in unique_annotations:
            annotation_str = f"{ann_text} << Type-{ann_type}, NCBI Label-{ncbi_label}, NCBI Id-{ncbi_id} >>"
            merged_text = merged_text.replace(ann_text, annotation_str)

        # # Append the annotations section
        # if annotations:
        #     merged_text += "\n\nAnnotations:\n"
        #     for ann in annotations:
        #         # Extract common fields
        #         annotation_text = ann["text"]
        #         annotation_type = ann["type"]
        #         annotation_label = ann["ncbi_label"]
        #         annotation_id = ann["ncbi_id"]
        #         annotation_offset = ann["offset"]
        #         annotation_length = ann["length"]
        #
        #         # Format the annotation information to be appended
        #         annotation_block = (
        #             f"Text - {annotation_text}"
        #             f"\n"
        #             f"Type - {annotation_type}"
        #             f"\n"
        #             f"{annotation_label} - {annotation_id}"
        #         )
        #         merged_text += f"{annotation_block}\n\n"

        return merged_text


# if __name__ == "__main__":
#     # Instantiate the merger and merge the annotations into the text
#     merger = InlineMerger()
#     text = chunk["chunk_text"]
#     annotations = chunk["chunk_annotations"]
#     merged_text = merger.merge(text, annotations)
#     print(merged_text)
