from fastapi import APIRouter

from src.pydantic_models.retrieval_models import (
    SearchRequest,
    ValuesSearchRequest,
)
from src.services.query_services import (
    pmc_articles_search_service,
    pmc_metadata_values_service,
)
from src.pubtator_utils.logs_handler.logger import SingletonLogger

logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

router = APIRouter()


@router.post("/search")
def search_pmc_articles(request: SearchRequest):
    chunks_by_article = pmc_articles_search_service(request)
    if not chunks_by_article:  # Check if the response is empty
        return {"message": "No articles found matching the query."}
    return {"Retrieved Articles": chunks_by_article}


@router.post("/get_distinct_values")
def get_distinct_values(request: ValuesSearchRequest):
    distinct_values = pmc_metadata_values_service(request)
    if not distinct_values:  # Check if the response is empty
        return {"message": "No distinct values found for the given criteria."}
    return {"Distinct Values": distinct_values}
