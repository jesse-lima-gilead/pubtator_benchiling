from datetime import datetime, timezone
from src.pubtator_utils.db_handler.alembic_models.document import Document
from src.pubtator_utils.db_handler.db import Session
import uuid

def insert_document_data(source, file_name, safe_file_name, file_path, document_grsar_id = None, size_bytes=None, workflow_id=None):
    file_type = file_name.split('.')[-1]
    document =  Document(
        document_grsar_id=str(document_grsar_id),
        document_name=file_name, #original filename
        workflow_id=workflow_id, #cmd
        document_type=file_type, #have to extract from org file name - eg.pdf,.docx
        document_grsar_name=safe_file_name,#safe file name
        source=source,
        source_path=file_path,
        created_dt=datetime.now(timezone.utc),
        last_update_dt=datetime.now(timezone.utc),
        document_file_size_in_bytes=size_bytes #add logic
    )
    with Session() as session:
        session.add(document)
        session.commit()

# def insert_document_data_with_starfish(valid_to,
#     volume_display_name,
#     file_extension_type,
#     mt, ct,
#     starfish_file_name,
#     starfish_size_unit,
#     starfish_file_size,
#     starfish_gid,
#     starfish_full_path,
#     volume,
#     uid,
#     valid_from,
#     starfish_object_id,
#     source, file_name, safe_file_name, file_path, size_bytes=None, workflow_id=None):
#     file_type = file_name.split('.')[-1]
#     document =  Document(
#         document_grsar_id="d1111111-2222-3333-4444-555555555555",
#         document_name=file_name, #original filename
#         workflow_id=workflow_id, #cmd
#         document_type=file_type, #have to extract from org file name - eg.pdf,.docx
#         document_grsar_name=safe_file_name,#safe file name
#         source=source,
#         source_path=file_path,
#         created_dt=datetime.now(),
#         last_update_dt=datetime.now(),
#         document_file_size_in_bytes=size_bytes,
#         starfish_document_valid_to = valid_to,
#         starfish_volume_display_name = volume_display_name,
#         starfish_file_extension_type = file_extension_type,
#         starfish_mt = mt,
#         starfish_ct = ct,
#         starfish_file_name = starfish_file_name,
#         starfish_size_unit = starfish_size_unit,
#         starfish_file_size = starfish_file_size,
#         starfish_gid = starfish_gid,
#         starfish_full_path = starfish_full_path, 
#         starfish_volume = volume,
#         starfish_uid = uid,
#         starfish_document_valid_from= valid_from,
#         starfish_object_id = starfish_object_id
#     )
#     session.add(document)
#     session.commit()