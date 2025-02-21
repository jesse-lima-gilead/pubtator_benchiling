from typing import List, Union

from pydantic import BaseModel


class PMC_Articles_Extractor_Request(BaseModel):
    query: str
    start_date: str
    end_date: str
    ret_max: int


class Metadata_Extractor_Request(BaseModel):
    storage_type: str
