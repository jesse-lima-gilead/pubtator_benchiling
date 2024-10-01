import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List


class SlidingWindowChunker:
    def __init__(
        self,
        xml_file_path,
        max_tokens_per_chunk=512,
        window_size: int = 512,
        stride: int = 256,
    ):
        self.xml_file_path = xml_file_path
        self.max_tokens_per_chunk = max_tokens_per_chunk
        self.window_size = window_size
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
                "infons": {
                    infon.get("key"): infon.text
                    for infon in annotation.findall("infon")
                },
            }
            passage_dict["annotations"].append(ann_dict)

        return passage_dict

    def chunk_passage(self, passage_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create chunks from a single passage using sliding window."""
        text = passage_dict["text"]
        base_offset = passage_dict["offset"]

        # Split text into words
        words = re.findall(r"\S+|\s+", text)

        chunks = []

        # Create chunks using a sliding window strategy
        for i in range(0, len(words), self.stride):
            chunk_words = words[i : i + self.window_size]
            chunk_text = "".join(chunk_words)
            chunk_offset = base_offset + sum(len(w) for w in words[:i])

            chunk = {
                "text": chunk_text,
                "offset": chunk_offset,
                "infons": passage_dict["infons"],
                "annotations": [],
            }

            # Include relevant annotations that fall within the chunk
            for ann in passage_dict["annotations"]:
                ann_start = ann["offset"] - base_offset
                ann_end = ann_start + ann["length"]
                chunk_start = sum(len(w) for w in words[:i])
                chunk_end = chunk_start + len(chunk_text)

                if (chunk_start <= ann_start < chunk_end) or (
                    chunk_start < ann_end <= chunk_end
                ):
                    chunk["annotations"].append(ann)

            chunks.append(chunk)

        return chunks

    def sliding_window_chunking(self) -> List[Dict[str, Any]]:
        """Chunk an entire BioC XML file using sliding window."""
        root = self.parse_bioc_xml()
        passages = self.extract_passages(root)

        all_chunks = []
        for passage in passages:
            passage_dict = self.passage_to_dict(passage)
            chunks = self.chunk_passage(passage_dict)
            all_chunks.extend(chunks)

        return all_chunks


# Example usage
# if __name__ == "__main__":
#     file_path = "path/to/your/bioc_xml_file.xml"
#
#     chunker = SlidingWindowChunker(window_size=512, stride=256)
#     chunks = chunker.chunk_file(file_path)
#
#     print(f"Number of chunks: {len(chunks)}")
#     print("\nFirst chunk:")
#     print(chunks[0])
