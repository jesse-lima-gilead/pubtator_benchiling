from data_processing.merging.merger_factory import (
    AnnotationMergeFactory,
)


def merge_annotations(
        text: str,
        annotations: list,
        merger_type: str,
):
    # Get the appropriate Text - Annotations Merger
    merger_factory = AnnotationMergeFactory(max_tokens_per_chunk=512)
    merger = merger_factory.get_merger(merger_type)
    merged_text = merger.merge(text, annotations)
    return merged_text
