import re
from collections import defaultdict
from typing import List, Dict, Any
import xml.etree.ElementTree as ET


class AnnotationAwareChunkerWithSlidingWindow:
    def __init__(
        self, xml_file_path: str, max_tokens_per_chunk: int = 512, stride: int = 256
    ):
        self.xml_file_path = xml_file_path
        self.max_tokens_per_chunk = max_tokens_per_chunk
        self.stride = stride

    def parse_bioc_xml(self) -> ET.Element:
        """Parse BioC XML file and return the root element."""
        tree = ET.parse(self.xml_file_path)
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
            ann_dict = {
                "id": annotation.get("id"),
                "text": annotation.find("text").text,
                "offset": int(annotation.find("location").get("offset")),
                "length": int(annotation.find("location").get("length")),
                "type": annotation.find("infon[@key='type']").text,
                "infons": {
                    infon.get("key"): infon.text
                    for infon in annotation.findall("infon")
                },
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

        # Split text into words
        words = re.findall(r"\S+|\s+", text)
        chunks = []

        for annotation_type, annotations in grouped_annotations.items():
            for i in range(0, len(words), self.stride):
                chunk_words = words[i : i + self.max_tokens_per_chunk]
                chunk_text = "".join(chunk_words)
                chunk_offset = base_offset + sum(len(w) for w in words[:i])

                # Create a new chunk for the current sliding window
                chunk = {
                    "text": chunk_text,
                    "offset": chunk_offset,
                    "infons": passage_dict["infons"],
                    "annotations": [],
                }

                # Include relevant annotations within the current window
                chunk_start = chunk_offset
                chunk_end = chunk_start + len(chunk_text)

                for ann in annotations:
                    ann_start = ann["offset"]
                    ann_end = ann_start + ann["length"]

                    if (chunk_start <= ann_start < chunk_end) or (
                        chunk_start < ann_end <= chunk_end
                    ):
                        chunk["annotations"].append(ann)

                # If annotations are present, add the chunk to the list
                if chunk["annotations"]:
                    chunks.append(chunk)

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
