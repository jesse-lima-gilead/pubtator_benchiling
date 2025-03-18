from fastapi import APIRouter

from src.pydantic_models.ingestion_models import (
    PMC_Articles_Extractor_Request,
    Metadata_Extractor_Request,
)
from src.services import ingestion_pipeline_services
from src.pubtator_utils.logs_handler.logger import SingletonLogger

logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

router = APIRouter()


@router.post("/extract_pmc_articles")
def extract_pmc_articles(request: PMC_Articles_Extractor_Request):
    ingestion_pipeline_services.pmc_articles_extractor_service(request)
    return {"message": "PMC articles extracted successfully"}


@router.post("/extract_metadata")
def extract_metadata(request: Metadata_Extractor_Request):
    ingestion_pipeline_services.articles_metadata_extractor_service(request)
    return {"message": "Metadata extracted successfully"}


@router.post("/convert_to_bioc")
def convert_to_bioc():
    ingestion_pipeline_services.pmc_to_bioc_converter_service()
    return {"message": "PMC articles converted to BioC"}


@router.post("/prettify_bioc")
def prettify_bioc():
    ingestion_pipeline_services.prettify_bioc_xml_service()
    return {"message": "BioC XML prettified"}


@router.post("/summarize_articles")
def summarize_articles():
    ingestion_pipeline_services.articles_summarizer()
    return {"message": "Articles summarized"}


@router.post("/ingestion_pipeline/")
def ingestion_pipeline():
    return ingestion_pipeline_services.ingestion_service()
