from pydantic import BaseModel


class SearchRequest(BaseModel):
    user_query: str
    metadata_filters: dict
    show_as_table: bool = False
    top_n: int = 5
    top_k: int = 100
    score_threshold: float = 0.7
    embeddings_model: str = "pubmedbert"


class MetadataExtractorRequest(BaseModel):
    storage_type: str


class ValuesSearchRequest(BaseModel):
    field_name: str
    field_value: str = None
    show_as_table: bool = False
