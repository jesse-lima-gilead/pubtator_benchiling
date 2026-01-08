from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from .base import Base


class Chunk(Base):
    __tablename__ = "chunk"

    chunk_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_grsar_id = Column(String, ForeignKey("document.document_grsar_id"))
    workflow_id = Column(String, ForeignKey("workflow.workflow_id"))
    chunk_sequence = Column(Integer, nullable=False)
    chunk_type = Column(String, nullable=False)
    vector_field_name = Column(String, nullable=False)
    chunk_annotations_count = Column(Integer)
    source = Column(String, nullable=False)
    chunk_creation_dt = Column(TIMESTAMP, nullable=False)
    chunk_creation_ds = Column(TIMESTAMP, nullable=False)