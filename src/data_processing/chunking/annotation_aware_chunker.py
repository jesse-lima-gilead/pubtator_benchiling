import xml.etree.ElementTree as ET


class AnnotationAwareChunker:
    def __init__(self, xml_file_path, max_tokens_per_chunk=512):
        self.xml_file_path = xml_file_path
        self.max_tokens_per_chunk = max_tokens_per_chunk

    def load_bioc_file(self):
        tree = ET.parse(self.xml_file_path)
        root = tree.getroot()
        return root

    def tokenize(self, text):
        """Simple tokenizer based on whitespace."""
        return text.split()

    def annotation_aware_chunking(self):
        root = self.load_bioc_file()
        chunks = []
        current_chunk_text = []
        current_annotations = []
        current_chunk_tokens = 0

        # Loop through passages
        for passage in root.findall(".//passage"):
            passage_text = passage.findtext("text").strip()
            tokens = self.tokenize(passage_text)

            for annotation in passage.findall("annotation"):
                annotation_offset = int(annotation.find("location").get("offset"))
                annotation_length = int(annotation.find("location").get("length"))
                annotation_data = {
                    "id": annotation.get("id"),
                    "type": annotation.findtext('infon[@key="type"]'),
                    "offset": annotation_offset,
                    "length": annotation_length,
                    "text": annotation.findtext("text"),
                }

                # If adding this annotation would exceed the token limit, finalize the chunk
                if current_chunk_tokens + len(tokens) > self.max_tokens_per_chunk:
                    chunks.append(
                        {
                            "text": " ".join(current_chunk_text),
                            "annotations": current_annotations,
                        }
                    )
                    current_chunk_text = []
                    current_annotations = []
                    current_chunk_tokens = 0

                # Add current passage text and annotations to the current chunk
                current_chunk_text.extend(tokens)
                current_annotations.append(annotation_data)
                current_chunk_tokens += len(tokens)

        # Add the last chunk if it exists
        if current_chunk_text:
            chunks.append(
                {
                    "text": " ".join(current_chunk_text),
                    "annotations": current_annotations,
                }
            )

        return chunks


# # Example usage
# annotation_chunker = BioCAnnotationAwareChunker('sample_bioc.xml')
# chunks = annotation_chunker.annotation_aware_chunking()
#
# # Print chunks for verification
# for i, chunk in enumerate(chunks):
#     print(f"Chunk {i + 1}: {chunk}")
