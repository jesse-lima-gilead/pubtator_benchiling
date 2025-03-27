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
    def bulk_insert(self, vectors_payloads: List[Any]):
        """Bulk insert vector documents."""
        pass

    @abstractmethod
    def search_vectors(
        self, query_vector: List[float], limit: int = 1, body: Dict[str, Any] = None
    ):
        """Search for vectors similar to the query vector."""
        pass

    @abstractmethod
    def search_with_filters(
        self, query_vector: List[float], top_k: int, filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Search vectors with additional filters.

        :param query_vector: The vector to search for.
        :param top_k: Number of top results to return.
        :param filters: Dictionary of field-based filters (e.g., {"year": 2024, "journal": "Nature"}).
        :return: List of matching results.
        """
        pass

    @abstractmethod
    def delete_index(self):
        """Delete the collection or index."""
        pass

    @abstractmethod
    def fuzzy_match(self, query: str, match_list: List[str], threshold: int):
        """Fuzzy match the query."""
        pass

    @abstractmethod
    def get_distinct_values(self, field_name: str) -> List[Dict[str, Any]]:
        """Get distinct values for a field."""
        pass

    @abstractmethod
    def delete_document_by_id(self, doc_id):
        """Deletes a single document by its ID."""
        pass

    @abstractmethod
    def delete_documents_by_query(self, query):
        """Deletes all documents that match the given query."""
        pass

    @abstractmethod
    def get_index_fields(self):
        """Fetch and print all fields of the index."""
        pass

    @abstractmethod
    def count_vectors(self) -> int:
        """Returns the total number of vector points stored in the OpenSearch index."""
        pass
