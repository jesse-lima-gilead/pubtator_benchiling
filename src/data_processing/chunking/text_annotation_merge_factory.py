from src.data_processing.chunking.inline_merger import InlineMerger
from src.data_processing.chunking.append_merger import AppendMerger
from src.data_processing.chunking.full_text_merger import FullTextMerger


class TextAnnotationMergeFactory:
    def __init__(self, xml_file_path, max_tokens_per_chunk=512):
        self.xml_file_path = xml_file_path
        self.max_tokens_per_chunk = max_tokens_per_chunk

    def get_merger(self, merger_type):
        """Factory method to return the appropriate chunker based on the chunker_type."""
        if merger_type == "append":
            return AppendMerger()
        elif merger_type == "inline":
            return InlineMerger()
        elif merger_type == "full_text":
            return FullTextMerger()
        else:
            raise ValueError(f"Unknown merger type: {merger_type}")
