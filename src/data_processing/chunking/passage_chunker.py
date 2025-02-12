import xml.etree.ElementTree as ET


class PassageChunker:
    def __init__(self, xml_file_path, file_handler):
        self.xml_file_path = xml_file_path
        self.file_handler = file_handler

    def load_bioc_file(self):
        tree = self.file_handler.parse_xml_file(self.xml_file_path)
        root = tree.getroot()
        return root

    def passage_based_chunking(self):
        root = self.load_bioc_file()
        chunks = []

        # Loop through each passage
        for passage in root.findall(".//passage"):
            passage_text = passage.findtext("text")
            annotations = []

            # Collect annotations within the passage
            for annotation in passage.findall("annotation"):
                id = annotation.get("id")
                type = annotation.findtext('infon[@key="type"]')
                offset = annotation.find("location").get("offset")
                length = annotation.find("location").get("length")
                text = annotation.findtext("text")
                if type and type.lower() == "gene":
                    identifier = annotation.findtext('infon[@key="NCBI Gene"]')
                elif type and type.lower() in ["species", "strain", "genus"]:
                    identifier = annotation.findtext('infon[@key="NCBI Taxonomy"]')
                elif type and type.lower() in ["chemical", "disease", "cellline"]:
                    identifier = annotation.findtext('infon[@key="identifier"]')
                elif (
                    type and annotation.findtext('infon[@key="Identifier"]') is not None
                ):
                    # Capture all other annotations with "Identifier" key (e.g., tmVar annotations)
                    identifier = annotation.findtext('infon[@key="Identifier"]')
                else:
                    additional_identifiers = []
                    for infon in annotation.findall("infon"):
                        key = infon.get("key")
                        if key and key.lower() not in [
                            "type",
                            "identifier",
                            "ncbi gene",
                            "ncbi taxonomy",
                        ]:
                            additional_identifiers.append(infon.text)

                    if additional_identifiers:
                        identifier = ", ".join(
                            additional_identifiers
                        )  # Join multiple identifiers if there are any

                annotation_data = {
                    "id": id,
                    "text": text,
                    "type": type,
                    "identifier": identifier,
                    "offset": int(offset),
                    "length": int(length),
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
