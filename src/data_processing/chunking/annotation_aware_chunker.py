import re
from collections import defaultdict
from typing import List, Dict, Any
import xml.etree.ElementTree as ET


class AnnotationAwareChunker:
    def __init__(
        self, xml_file_path: str, file_handler, max_tokens_per_chunk: int = 512
    ):
        self.xml_file_path = xml_file_path
        self.max_tokens_per_chunk = max_tokens_per_chunk
        self.file_handler = file_handler

    def parse_bioc_xml(self) -> ET.Element:
        """Parse BioC XML file and return the root element."""
        tree = self.file_handler.parse_xml_file(self.xml_file_path)
        return tree.getroot()

    def extract_passages(self, root: ET.Element) -> List[ET.Element]:
        """Extract all passage elements from the BioC XML."""
        return root.findall(".//passage")

    def passage_to_dict(self, passage: ET.Element) -> Dict[str, Any]:
        """Convert a passage element to a dictionary."""
        passage_dict = {
            "text": passage.find("text").text,
            "offset": int(passage.find("offset").text),
            "infons": {
                infon.get("key"): infon.text for infon in passage.findall("infon")
            },
            "annotations": [],
        }

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
            elif type and annotation.findtext('infon[@key="Identifier"]') is not None:
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

            ann_dict = {
                "id": id,
                "text": text,
                "type": type,
                "identifier": identifier,
                "offset": int(offset),
                "length": int(length),
            }
            passage_dict["annotations"].append(ann_dict)

        return passage_dict

    def group_annotations_by_type(
        self, passage_dict: Dict[str, Any]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Group annotations by their 'type'."""
        grouped_annotations = defaultdict(list)

        for annotation in passage_dict["annotations"]:
            annotation_type = annotation["type"]
            grouped_annotations[annotation_type].append(annotation)

        return grouped_annotations

    def create_chunks_from_groups(
        self,
        passage_dict: Dict[str, Any],
        grouped_annotations: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """Create chunks from the grouped annotations, keeping the same type in one chunk."""
        text = passage_dict["text"]
        base_offset = passage_dict["offset"]

        chunks = []

        # Create a chunk for each annotation group (same type)
        for annotation_type, annotations in grouped_annotations.items():
            # Sort annotations by their offsets to keep order
            annotations = sorted(annotations, key=lambda ann: ann["offset"])

            # Start building a chunk for this type
            chunk_text_parts = []
            chunk_annotations = []

            last_end = 0

            for ann in annotations:
                # Extract the text segment around the annotation
                start = ann["offset"] - base_offset
                end = start + ann["length"]

                # Add the text before the annotation
                chunk_text_parts.append(text[last_end:start])

                # Add the annotation text itself
                chunk_text_parts.append(text[start:end])

                # Add the annotation to the chunk
                chunk_annotations.append(ann)

                last_end = end

            # Add any remaining text after the last annotation
            chunk_text_parts.append(text[last_end:])

            # Join the parts to create the full chunk text
            chunk_text = "".join(chunk_text_parts)

            chunk = {
                "text": chunk_text,
                "offset": base_offset,
                "infons": passage_dict["infons"],
                "annotations": chunk_annotations,
            }

            chunks.append(chunk)

        return chunks

    def chunk_passage(self, passage_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk the passage by grouping annotations of the same type."""
        grouped_annotations = self.group_annotations_by_type(passage_dict)
        return self.create_chunks_from_groups(passage_dict, grouped_annotations)

    def annotation_aware_chunking(self) -> List[Dict[str, Any]]:
        """Chunk an entire BioC XML file by grouping annotations of the same type."""
        root = self.parse_bioc_xml()
        passages = self.extract_passages(root)

        all_chunks = []
        for passage in passages:
            passage_dict = self.passage_to_dict(passage)
            chunks = self.chunk_passage(passage_dict)
            all_chunks.extend(chunks)

        return all_chunks
