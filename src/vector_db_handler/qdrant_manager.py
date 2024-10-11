import json
from typing import Dict, List

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchExcept,
    MatchValue,
    PayloadSelector,
    PointIdsList,
    PointStruct,
    VectorParams,
)

from src.utils.logger import SingletonLogger

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class QdrantManager:
    def __init__(
        self, host: str, port: int, collection_name: str, vector_size: int = 1024, distance_metric: str = "COSINE"
    ):
        self.collection_name = collection_name
        self.client = QdrantClient(host=host, port=port)
        self.vector_size = vector_size
        self.distance_metric = distance_metric

    def check_if_collection_exists(self) -> bool:
        result = False
        try:
            # Check if collection exists
            if self.client.get_collection(self.collection_name):
                result = True
        except Exception as e:
            logger.info(f"Collection {self.collection_name} does not exist. Creating now!")
        return result

    def create_collection(self):
        # Create a new collection
        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=self.vector_size,  # Adjust based on your embedding model
                distance=getattr(Distance, self.distance_metric),  # Distance metric (COSINE, EUCLID, etc.)
            ),
        )
        logger.info(f"Collection {self.collection_name} created")

    def insert_vector(self, vector: List[float], payload: Dict):
        point_id = payload.get("chunk_id", None)
        if point_id is None:
            raise ValueError("Payload must include an 'id' field for upserting.")

        point = PointStruct(id=point_id, vector=vector, payload=payload)

        self.client.upsert(collection_name=self.collection_name, points=[point])

    def search_vectors(self, query_vector: List[float], limit: int = 1):
        return self.client.search(collection_name=self.collection_name, query_vector=query_vector, limit=limit)

    def delete_collection(self):
        self.client.delete_collection(collection_name=self.collection_name)

    def delete_points_by_key(self, key: str, value: str):
        # logger.info(f'{key}\n{value}')
        filter_query = Filter(must=[FieldCondition(key=key, match=MatchValue(value=value))])

        # Retrieve Points(Chunks) that match the filter:
        scroll_result = self.client.scroll(collection_name=self.collection_name, scroll_filter=filter_query, limit=1000)

        # Extract PointIDs from Scroll Result:
        point_ids = [point.id for point in scroll_result[0]]

        # Delete the extracted Points using PointIds:
        if point_ids:
            self.client.delete(
                collection_name=self.collection_name,
                points_selector=PointIdsList(points=point_ids),
            )
            logger.info(f"Deleted {len(point_ids)} points with {key} '{value}'.")
        else:
            logger.info(f"No points found with {key} '{value}'.")

    def get_distinct_key_values(self, key: str, limit: int = 1000) -> List[str]:
        """
        Fetch distinct key(e.g.file names) values from the specified Qdrant collection.
        :param key: Key whose distinct values should be returned. e.g. file_name
        :param limit: Number of points to retrieve per batch.
        :return: List of distinct key values (e.g. file names).
        """
        distinct_values = set()

        # Initialize offset and limit for Pagination:
        offset = 0
        limit = limit

        while True:
            # Perform a scroll request to retrieve points with limited payload
            scroll_result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=Filter(
                    must=[FieldCondition(key=key, match=MatchExcept(**{"except": ["None"]}))],
                ),  # No additional filter
                # with_payload=PayloadSelector(include=[key]),  # Select only the "key" field
                offset=offset,
                limit=limit,
            )

            # Extract points and the next offset
            points, next_offset = scroll_result

            # Extract key (file names) from the points and add to the set
            for point in points:
                # Ensure the point has a payload
                if point.payload:
                    # Extract the value associated with the key
                    value = point.payload.get(key)
                    if value:
                        distinct_values.add(value)

            # Check if we have reached the end of the collection
            if not next_offset:
                break

            # Update offset for the next batch
            offset = next_offset

        return list(distinct_values)

    def __del__(self):
        # Optional: Clean up resources if needed
        pass