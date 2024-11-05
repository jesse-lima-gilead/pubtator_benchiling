from data_processing.merging.inline_merger import InlineMerger
from data_processing.merging.append_merger import AppendMerger
from data_processing.merging.full_text_merger import FullTextMerger
from data_processing.merging.prepend_merger import PrependMerger


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
        elif merger_type == "prepend":
            return PrependMerger()
        elif merger_type == "full_text":
            return FullTextMerger()
        else:
            raise ValueError(f"Unknown merger type: {merger_type}")
