class InlineMerger:
    def merge(self, text: str, annotations: list):
        """
        Merges annotations inline with the text based on their offset and length.

        Args:
        - chunk: a dictionary containing 'text' and 'annotations'

        Returns:
        - Merged text with annotations placed inline.
        """

        # Sort annotations by the offset to avoid disrupting other offsets
        annotations_sorted = sorted(annotations, key=lambda ann: ann["offset"])

        # Offset adjustment to account for the increasing length of the inserted annotations
        offset_adjustment = 0

        # Track already annotated words in this chunk to avoid duplicates
        annotated_offsets = set()

        for ann in annotations_sorted:
            # Extract annotation details
            annotation_text = ann["text"]
            annotation_type = ann["type"]
            annotation_label = ann["ncbi_label"]
            annotation_id = ann["ncbi_id"]
            annotation_offset = int(ann["offset"])
            annotation_length = int(ann["length"])

            # Skip if the annotation overlaps with an already processed one
            if annotation_offset in annotated_offsets:
                continue

            # Add annotation to processed list
            annotated_offsets.update(
                range(annotation_offset, annotation_offset + annotation_length)
            )

            # Insert the annotation inline into the text
            annotated_text = f"{annotation_text} {annotation_label}"
            text = (
                text[:annotation_offset]
                + annotated_text
                + text[annotation_offset + annotation_length :]
            )

            # Update the offset adjustment to account for the added length
            offset_adjustment += len(annotated_text) - annotation_length

        return text


# if __name__ == "__main__":
#     # Instantiate the merger and merge the annotations into the text
#     merger = InlineMerger()
#     text = chunk["chunk_text"]
#     annotations = chunk["chunk_annotations"]
#     merged_text = merger.merge(text, annotations)
#     print(merged_text)
