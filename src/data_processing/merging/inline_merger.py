import re


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
        unique_annotations = {
            (ann["text"], ann["type"], ann["ncbi_label"], ann["ncbi_id"])
            for ann in annotations
        }

        # Replace text using replace() to avoid offset disruption
        for ann_text, ann_type, ncbi_label, ncbi_id in unique_annotations:
            annotation_str = f"{ann_text} << Type-{ann_type}, NCBI Label-{ncbi_label}, NCBI Id-{ncbi_id} >>"

            # Simple replace
            # merged_text = merged_text.replace(ann_text, annotation_str)

            # Use word boundaries to match exact words
            merged_text = re.sub(
                rf"\b{re.escape(ann_text)}\b", annotation_str, merged_text
            )

        return merged_text


# if __name__ == "__main__":
#     # Instantiate the merger and merge the annotations into the text
#     merger = InlineMerger()
#     text = chunk["chunk_text"]
#     annotations = chunk["chunk_annotations"]
#     merged_text = merger.merge(text, annotations)
#     print(merged_text)
