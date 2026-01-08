from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey, Float
from sqlalchemy.dialects.postgresql import UUID
import uuid
from .base import Base

class Document(Base):
    __tablename__ = "document"

    document_grsar_id = Column(String, primary_key=True)
    document_name = Column(String, nullable=False)
    workflow_id = Column(String, ForeignKey("workflow.workflow_id"))
    document_type = Column(String)
    document_grsar_name = Column(String)
    source = Column(String, nullable=False)
    source_path = Column(String)
    created_dt = Column(TIMESTAMP)
    last_update_dt = Column(TIMESTAMP)
    document_file_size_in_bytes = Column(Integer)
    starfish_document_valid_to = Column(Integer)
    starfish_volume_display_name= Column(String)
    starfish_file_extension_type = Column(String)
    starfish_mt = Column(Integer)
    starfish_ct = Column(Integer)
    starfish_file_name= Column(String)
    starfish_size_unit = Column(String)
    starfish_file_size = Column(Integer)
    starfish_gid = Column(Integer)
    starfish_full_path = Column(String)
    starfish_volume = Column(String)
    starfish_uid = Column(Integer)
    starfish_document_valid_from = Column(Float)
    starfish_object_id = Column(Integer)