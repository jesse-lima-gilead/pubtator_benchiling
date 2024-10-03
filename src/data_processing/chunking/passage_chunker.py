import xml.etree.ElementTree as ET


class PassageChunker:
    def __init__(self, xml_file_path):
        self.xml_file_path = xml_file_path

    def load_bioc_file(self):
        tree = ET.parse(self.xml_file_path)
        root = tree.getroot()
        return root

    def passage_based_chunking(self):
        root = self.load_bioc_file()
        chunks = []

        # Loop through each passage
        for passage in root.findall(".//passage"):
            passage_text = passage.findtext("text").strip()
            annotations = []

            # Collect annotations within the passage
            for annotation in passage.findall("annotation"):
                annotation_data = {
                    "id": annotation.get("id"),
                    "type": annotation.findtext('infon[@key="type"]'),
                    "offset": annotation.find("location").get("offset"),
                    "length": annotation.find("location").get("length"),
                    "text": annotation.findtext("text"),
                }
                annotations.append(annotation_data)

            # Create a chunk containing passage text and annotations
            chunk = {
                "text": passage_text,
                "offset": int(passage.find("offset").text),
                "infons": {
                    infon.get("key"): infon.text for infon in passage.findall("infon")
                },
                "annotations": annotations,
            }
            chunks.append(chunk)

        return chunks


# # Example usage
# chunker = BioCPassageChunker('sample_bioc.xml')
# chunks = chunker.passage_based_chunking()
#
# # Print chunks for verification
# for i, chunk in enumerate(chunks):
#     print(f"Chunk {i + 1}: {chunk}")
