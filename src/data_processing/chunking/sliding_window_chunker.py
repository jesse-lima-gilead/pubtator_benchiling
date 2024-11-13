import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List


class SlidingWindowChunker:
    def __init__(
        self,
        xml_file_path,
        max_tokens_per_chunk=512,
        **kwargs,
    ):
        self.xml_file_path = xml_file_path
        self.max_tokens_per_chunk = max_tokens_per_chunk
        self.window_size = kwargs.get("window_size", 512)
        self.stride = kwargs.get("stride", 256)

    def parse_bioc_xml(self) -> ET.Element:
        """Parse BioC XML file and return the root element."""
        tree = ET.parse(self.xml_file_path)
        return tree.getroot()

    import re
    import xml.etree.ElementTree as ET

    def remove_unwanted_passages(self, root: ET.Element, unwanted_patterns: list[str]) -> None:
        """
        Remove passages with <infon key="type"> that match any unwanted types from a list of patterns.

        Args:
            root (ET.Element): Root element of the BioC XML structure.
            unwanted_patterns (list[str]): List of regex patterns for passage types to be removed.
        """
        # Compile all unwanted patterns into a list of regex patterns for case-insensitive matching
        regex_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in unwanted_patterns]

        passages = root.findall(".//passage")
        # print("Initial Passages in BioC XML:", len(passages))

        # Use list comprehension to filter out passages matching any of the unwanted patterns
        passages_to_keep = [
            passage for passage in passages
            if not (
                    passage.find(".//infon[@key='type']") is not None and
                    any(regex.search(passage.find(".//infon[@key='type']").text) for regex in regex_patterns)
            )
        ]

        # print(f"Remaining Passages: {len(passages_to_keep)}")

        # Optionally update the root element with the filtered passages
        root[:] = passages_to_keep
    

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
            if type.lower() == "species":
                ncbi_label = "NCBI Taxonomy"
                ncbi_id = annotation.findtext('infon[@key="NCBI Taxonomy"]')
            elif type.lower() == "gene":
                ncbi_label = "NCBI Gene"
                ncbi_id = annotation.findtext('infon[@key="NCBI Gene"]')
            else:
                ncbi_label = "NCBI ID"
                ncbi_id = "N/A"

            ann_dict = {
                "id": id,
                "text": text,
                "type": type,
                "ncbi_label": ncbi_label,
                "ncbi_id": ncbi_id,
                "offset": int(offset),
                "length": int(length),
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
        # unwanted_patterns = [r"acknowledge.*", r"conflict of interest.*", r"disclaimer.*"]
        unwanted_patterns = [r"acknowledge.*"]
        self.remove_unwanted_passages(root, unwanted_patterns=unwanted_patterns)
        passages = self.extract_passages(root)

        all_chunks = []
        for passage in passages:
            passage_dict = self.passage_to_dict(passage)
            chunks = self.chunk_passage(passage_dict)
            all_chunks.extend(chunks)

        return all_chunks


# # Example usage
# if __name__ == "__main__":
#     xml_file_path = "../../../test_data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/PMC_7614604.xml"
#
#     chunker = SlidingWindowChunker(xml_file_path=xml_file_path, window_size=512, stride=256)
#     chunks = chunker.sliding_window_chunking()
#
#     print(f"Number of chunks: {len(chunks)}")
#     print("\nFirst chunk:")
#     print(chunks[0])
