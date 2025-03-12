from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseVectorDBHandler(ABC):
    def __init__(self, vector_db_params: dict, index_params: dict):
        self.vector_db_params = vector_db_params
        self.index_params = index_params

    @abstractmethod
    def check_if_index_exists(self) -> bool:
        """Check if the collection or index exists."""
        pass

    @abstractmethod
    def create_index(self):
        """Create the collection or index with the appropriate mapping/settings."""
        pass

    @abstractmethod
    def insert_vector(self, vector: List[float], payload: Dict[str, Any]):
        """Insert a single vector document."""
        pass

    @abstractmethod
    def insert_vectors(self, vectors_payloads: List[Any]):
        """Bulk insert vector documents."""
        pass

    @abstractmethod
    def search_vectors(
        self, query_vector: List[float], limit: int = 1, body: Dict[str, Any] = None
    ):
        """Search for vectors similar to the query vector."""
        pass

    @abstractmethod
    def delete_index(self):
        """Delete the collection or index."""
        pass
