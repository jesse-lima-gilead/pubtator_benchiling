from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
import uuid
from .base import Base


class Workflow(Base):
    __tablename__ = "workflow"

    workflow_id = Column(String, primary_key=True)
    source = Column(String)
    run_type = Column(String)
    workflow_start_ts = Column(TIMESTAMP)
    workflow_stop_ts = Column(TIMESTAMP)
    extraction_start_ts = Column(TIMESTAMP)
    extraction_stop_ts = Column(TIMESTAMP)
    ingestion_start_ts = Column(TIMESTAMP)
    ingestion_stop_ts = Column(TIMESTAMP)
    ner_start_ts = Column(TIMESTAMP)
    ner_stop_ts = Column(TIMESTAMP)
    chunk_embedding_start_ts = Column(TIMESTAMP)
    chunk_embedding_stop_ts = Column(TIMESTAMP)
    status = Column(String)
    error_message = Column(String)