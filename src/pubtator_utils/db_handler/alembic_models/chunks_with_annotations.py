from sqlalchemy import Column, Integer, String, JSON, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import uuid

Base = declarative_base()


class ChunkWithAnnotations(Base):
    __tablename__ = "chunks_with_annotations"

    article_id = Column(String, nullable=False)
    chunk_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    chunk_sequence = Column(String, nullable=False)
    chunk_name = Column(String, nullable=False)
    chunk_length = Column(Integer, nullable=False)
    token_count = Column(Integer, nullable=False)
    chunk_annotations_count = Column(Integer, nullable=False)
    chunk_annotations_ids = Column(ARRAY(String), nullable=False)
    genes = Column(String, nullable=False)
    species = Column(String, nullable=False)
    cell_lines = Column(String, nullable=False)
    strains = Column(String, nullable=False)
    diseases = Column(String, nullable=False)
    chemicals = Column(String, nullable=False)
    variants = Column(String, nullable=False)
    chunk_offset = Column(Integer, nullable=False)
    chunk_infons = Column(JSON, nullable=False)
    chunker_type = Column(String, nullable=False)
    merger_type = Column(String, nullable=False)
    aioner_model = Column(String, nullable=False)
    gnorm2_model = Column(String, nullable=False)
