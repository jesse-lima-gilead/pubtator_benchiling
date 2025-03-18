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
    return {"Retrieved Articles": chunks_by_article}


@router.post("/get_distinct_values")
def get_distinct_values(request: ValuesSearchRequest):
    distinct_values = pmc_metadata_values_service(request)
    return {"Distinct Values": distinct_values}
