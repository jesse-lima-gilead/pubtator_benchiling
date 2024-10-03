from src.data_processing.chunking.annotation_aware_chunker import AnnotationAwareChunker
from src.data_processing.chunking.passage_chunker import PassageChunker
from src.data_processing.chunking.sliding_window_chunker import SlidingWindowChunker
from src.data_processing.chunking.grouped_annotation_sliding_window_chunker import (
    AnnotationAwareChunkerWithSlidingWindow,
)


class ChunkerFactory:
    def __init__(self, xml_file_path, max_tokens_per_chunk=512):
        self.xml_file_path = xml_file_path
        self.max_tokens_per_chunk = max_tokens_per_chunk

    def get_chunker(self, chunker_type):
        """Factory method to return the appropriate chunker based on the chunker_type."""
        if chunker_type == "passage":
            return PassageChunker(self.xml_file_path)
        elif chunker_type == "annotation_aware":
            return AnnotationAwareChunker(self.xml_file_path, self.max_tokens_per_chunk)
        elif chunker_type == "sliding_window":
            return SlidingWindowChunker(self.xml_file_path, self.max_tokens_per_chunk)
        elif chunker_type == "grouped_annotation_aware_sliding_window":
            return AnnotationAwareChunkerWithSlidingWindow(
                self.xml_file_path, self.max_tokens_per_chunk
            )
        # elif chunker_type == 'sentence':
        #     return SentenceChunker(self.xml_file_path)
        else:
            raise ValueError(f"Unknown chunker type: {chunker_type}")
