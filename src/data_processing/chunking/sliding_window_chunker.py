import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

# from src.data_processing.embedding.embeddings_handler import (
#     get_embeddings,
#     get_model_info,
#     save_embeddings_details_to_json,
# )
# import math
# from transformers import AutoTokenizer
# def get_token_count(chunk_text: str):
#     model_info = get_model_info("pubmedbert")
#     model_path = model_info[0]
#     tokenizer = AutoTokenizer.from_pretrained(model_path)
#     rec_tokens = tokenizer.tokenize(chunk_text)
#     print(rec_tokens)
#     return len(rec_tokens)


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
        self.stride = self.window_size // 2

    def parse_bioc_xml(self) -> ET.Element:
        """Parse BioC XML file and return the root element."""
        tree = ET.parse(self.xml_file_path)
        return tree.getroot()

    import xml.etree.ElementTree as ET

    def remove_unwanted_passages(
        self, root: ET.Element, unwanted_patterns: list[str]
    ) -> None:
        """
        Remove passages with <infon key="type"> that match any unwanted types from a list of patterns.

        Args:
            root (ET.Element): Root element of the BioC XML structure.
            unwanted_patterns (list[str]): List of regex patterns for passage types to be removed.
        """
        # Compile all unwanted patterns into a list of regex patterns for case-insensitive matching
        regex_patterns = [
            re.compile(pattern, re.IGNORECASE) for pattern in unwanted_patterns
        ]

        passages = root.findall(".//passage")
        # print("Initial Passages in BioC XML:", len(passages))

        # Use list comprehension to filter out passages matching any of the unwanted patterns
        passages_to_keep = [
            passage
            for passage in passages
            if not (
                passage.find(".//infon[@key='type']") is not None
                and any(
                    regex.search(passage.find(".//infon[@key='type']").text)
                    for regex in regex_patterns
                )
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

    def process_chunks(
        self, words, start_index, is_final_chunk, base_offset, passage_dict
    ):
        if is_final_chunk:
            chunk_words = words[start_index:]
        else:
            chunk_words = words[start_index : start_index + self.window_size]
        chunk_text = "".join(chunk_words)
        chunk_offset = base_offset + sum(len(w) for w in words[:start_index])
        # cnt = 0
        # for word in chunk_words:
        #     if word.strip() != "":
        #         cnt += 1
        # print("words list count: ", len(chunk_words), " so should have atleast: ",len(chunk_words) // 2,)
        # print(chunk_words)
        # print("expected words: ",self.window_size // 2," got words: ",cnt, " but got token count: ",get_token_count(chunk_text),)
        # print("NO. of Words in the CHUNK: ", cnt)

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
            chunk_start = sum(len(w) for w in words[:start_index])
            chunk_end = chunk_start + len(chunk_text)

            if (chunk_start <= ann_start < chunk_end) or (
                chunk_start < ann_end <= chunk_end
            ):
                chunk["annotations"].append(ann)

        return chunk

    def chunk_passage(self, passage_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create chunks from a single passage using sliding window."""
        # print()
        # print("***********")
        # print("NEW PASSAGE:**************")
        # print("***********")
        text = passage_dict["text"]
        base_offset = passage_dict["offset"]

        # Split text into words
        words = re.findall(r"\S+|\s+", text)

        chunks = []

        # Create chunks using a sliding window strategy
        for i in range(0, len(words), self.stride):
            # print()
            start_index = i
            end_index = start_index + self.window_size
            # if end_index >= len(words):
            chunk_words = words[i : i + self.window_size]
            # handle if the chunk size is smaller than or equal to window size itself
            if len(chunk_words) < self.window_size:
                is_final_chunk = True
                # print("LAST EDGE:*********")
                chunk = self.process_chunks(
                    words, start_index, is_final_chunk, base_offset, passage_dict
                )
                chunks.append(chunk)
                break

            else:
                # if the words present to the right of the current chunk is <= stride(256) length
                # lets just include it in the current passage itself
                still_present_in_the_right = words[
                    end_index : end_index + self.window_size
                ]
                if len(still_present_in_the_right) <= self.stride:
                    is_final_chunk = True
                    # print("LAST BUT ONE EDGE:*********")
                    chunk = self.process_chunks(
                        words, start_index, is_final_chunk, base_offset, passage_dict
                    )
                    chunks.append(chunk)
                    break

                # simple case where the words present in the right is greater than stride(256) length which can be handled in the next iteration
                else:
                    is_final_chunk = False
                    chunk = self.process_chunks(
                        words, start_index, is_final_chunk, base_offset, passage_dict
                    )
                    chunks.append(chunk)

                # # allowing to run normally since we have the window size atleast
                # # if the chunk size becomes smaller than window size will be handled on top
                # is_final_chunk = False
                # chunk = self.process_chunks(words, start_index, is_final_chunk, base_offset, passage_dict)
                # chunks.append(chunk)

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
#     xml_file_path = "../../../data/ner_processed/gnorm2_annotated/PMC_5724586.xml"
#
#     summary = "In 2410 older post-myocardial infarction patients, high body mass index and waist circumference were associated with more rapid annual kidney function decline of 0.35 and 0.21 ml/min/1.73m2 per BMI increment in men and women despite optimal drug treatment. Obese patients declined 30-45% faster versus normal weight individuals."
#     # summary = "Phosphorylation of PDK-1, AKT, mTOR, p70S6K, and S6 was elevated in breast cancer cell lines and primary tumors compared to normal breast epithelial cells. Moderate-to-high phosphorylation of PDK-1 occurred in 81% of invasive breast carcinomas and was significantly associated with invasiveness. Phosphorylation of putative PDK-1 downstream targets like AKT, mTOR, p70S6K, and S6 was also increased and correlated with PDK-1 phosphorylation and breast cancer. Up to 86% of metastatic tumors had elevated PDK-1 phosphorylation, suggesting PDK-1 activation may promote breast cancer progression. Inhibition of PDK-1 and its downstream signaling cascade may provide additional therapeutic strategies."
#     tokens_len = get_token_count(summary)
#     print(tokens_len)
#     max_tokens = 512
#     tokens_left = max_tokens - tokens_len
#     print(tokens_left, " tokens left")
#     buffer = math.floor(tokens_left * 0.15)
#     print(buffer, "buffer")
#     tokens_left_with_buffer = tokens_left - buffer
#     print(tokens_left_with_buffer, "tokens left aftwr buffer")
#     words_left = math.floor(tokens_left_with_buffer * 0.75)
#     print(words_left, "words left aftwr buffer")
#     window_size = 2 * words_left
#     print(window_size, "window size")
#
#     chunker = SlidingWindowChunker(xml_file_path=xml_file_path, window_size=window_size)
#     chunks = chunker.sliding_window_chunking()
#
#     print(f"\nNumber of chunks: {len(chunks)}")
#     print("\nFirst chunk:")
#     print(chunks[0]['annotations'])
#     print(len(chunks[0]['annotations']))


## Limit 512 Tokens ~ 512*0.75=384 Words
## Summary ~ 80 Words without spaces ~ 80*1.34=107 Tokens
## 384-80=304 Words Left for the chunk text ~ 300 Words
## Our words have spaces, we are counting 1 space as 1 word
## Thus in 300 limit, we can have actual ~150 words, ie 150*1.34=201 Tokens

## 500 Words ie 250 words and 250 spaces
## 250 words = 250*1.34=335 Tokens

## Total tokens = 335+107=442 Tokens
## Buffer = 512-442=70 Tokens ie. 70*0.75=52 Words buffer

## Buffer Usage:
## To Store the annotations
## To Store words that generates a lot of tokens
