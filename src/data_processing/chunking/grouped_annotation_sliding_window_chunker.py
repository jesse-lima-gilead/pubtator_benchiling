import re
from collections import defaultdict
from typing import List, Dict, Any
import xml.etree.ElementTree as ET


class AnnotationAwareChunkerWithSlidingWindow:
    def __init__(
        self,
        xml_file_path,
        file_handler,
        max_tokens_per_chunk=512,
        **kwargs,
    ):
        self.xml_file_path = xml_file_path
        self.max_tokens_per_chunk = max_tokens_per_chunk
        self.file_handler = file_handler
        self.window_size = kwargs.get("window_size", 512)
        self.stride = self.window_size // 2

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

    def create_sliding_window_chunks(
        self,
        passage_dict: Dict[str, Any],
        grouped_annotations: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """Create sliding window chunks from the grouped annotations."""
        text = passage_dict["text"]
        base_offset = passage_dict["offset"]

        # Split text into words (spaces included for accurate chunk reconstruction)
        words = re.findall(r"\S+|\s+", text)
        chunks = []

        # Create a set to track unique chunks to avoid duplicates
        unique_chunks = set()

        for i in range(0, len(words), self.stride):
            chunk_words = words[i : i + self.window_size]
            chunk_text = "".join(chunk_words)
            chunk_offset = base_offset + sum(len(w) for w in words[:i])

            # Prepare to store annotations for this chunk
            chunk_annotations = defaultdict(list)
            annotations_found = False  # Flag to check if any annotations are found

            # Check annotations for each annotation type
            for annotation_type, annotations in grouped_annotations.items():
                # Initialize a chunk with the relevant text and base info
                chunk = {
                    "text": chunk_text,
                    "offset": chunk_offset,
                    "infons": passage_dict["infons"],
                    "annotations": [],
                    "annotation_type": annotation_type,
                }

                chunk_start = chunk_offset
                chunk_end = chunk_start + len(chunk_text)

                # Check annotations for the current type
                for ann in annotations:
                    ann_start = ann["offset"]
                    ann_end = ann_start + ann["length"]

                    # Check if annotation falls within the chunk boundaries
                    if (chunk_start <= ann_start < chunk_end) or (
                        chunk_start < ann_end <= chunk_end
                    ):
                        chunk["annotations"].append(ann)
                        annotations_found = True  # Set flag if annotations are found
                        chunk_annotations[annotation_type].append(ann)

                # If the chunk has relevant annotations, add it to unique chunks
                if chunk["annotations"]:
                    # Create a unique identifier for the chunk (text and type)
                    chunk_id = (chunk_text, annotation_type)
                    if chunk_id not in unique_chunks:
                        chunks.append(chunk)  # Add the chunk
                        unique_chunks.add(chunk_id)  # Mark this chunk as seen

            # Create an empty chunk if no annotations were found
            if not annotations_found:
                empty_chunk = {
                    "text": chunk_text,
                    "offset": chunk_offset,
                    "infons": passage_dict["infons"],
                    "annotations": [],
                }
                empty_chunk_id = (
                    chunk_text,
                    "empty",
                )  # Unique identifier for empty chunk
                if empty_chunk_id not in unique_chunks:
                    chunks.append(empty_chunk)  # Add empty chunk
                    unique_chunks.add(empty_chunk_id)  # Mark as seen

        return chunks

    def chunk_passage(self, passage_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk the passage using sliding window and grouping annotations of the same type."""
        grouped_annotations = self.group_annotations_by_type(passage_dict)
        return self.create_sliding_window_chunks(passage_dict, grouped_annotations)

    def grouped_annotation_aware_sliding_window_chunking(self) -> List[Dict[str, Any]]:
        """Chunk an entire BioC XML file using sliding window and grouping annotations of the same type."""
        root = self.parse_bioc_xml()
        passages = self.extract_passages(root)

        all_chunks = []
        for passage in passages:
            passage_dict = self.passage_to_dict(passage)
            chunks = self.chunk_passage(passage_dict)
            all_chunks.extend(chunks)

        return all_chunks
