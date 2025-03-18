from pydantic import BaseModel


class SearchRequest(BaseModel):
    user_query: str
    metadata_filters: dict


class MetadataExtractorRequest(BaseModel):
    storage_type: str


class ValuesSearchRequest(BaseModel):
    field_name: str
