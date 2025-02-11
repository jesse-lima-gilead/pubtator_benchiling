from fastapi import APIRouter

from src.pydantic_models import CreateVectorDBRequest, DeleteDocRequest
from src.services import document_services
from src.utils.logger import SingletonLogger

logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

router = APIRouter()


@router.post("/ingest_documents/")
def list_existing_documents():
    return document_services.ingestion_service()


@router.post("/annotate_document/")
def upload_document():
    return document_services.annotation_service()


@router.post("/chunk_document/")
def populate_vector_db(request: CreateVectorDBRequest):
    return document_services.chunking_service(request)


@router.post("/embed_document/")
def populate_vector_db(request: CreateVectorDBRequest):
    return document_services.embedding_service(request)
