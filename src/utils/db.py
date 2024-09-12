# src/db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.alembic_models.chunks import Base, Chunk

# Create a SQLAlchemy engine
engine = create_engine("postgresql://postgres:postgres@localhost/policy_bot")

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a Session instance
session = Session()
