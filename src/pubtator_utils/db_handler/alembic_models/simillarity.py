from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ChunkSimilarity(Base):
    __tablename__ = "chunk_similarity"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_query = Column(String, nullable=False)
    embed_model = Column(String(100), nullable=False)
    annotation_model = Column(String(100), nullable=False)
    chunking_strategy = Column(String(100), nullable=False)
    annotation_placement_strategy = Column(String(100), nullable=False)
    contains_summary = Column(String(3), nullable=False)  # "Yes" or "No"
    chunk_sequence = Column(String(10), nullable=False)
    similarity_score = Column(Float, nullable=False)
    chunk_file = Column(String(200), nullable=False)
