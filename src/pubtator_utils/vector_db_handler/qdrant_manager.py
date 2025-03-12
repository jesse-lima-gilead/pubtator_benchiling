import os
from typing import Dict, List, Any
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchExcept,
    MatchValue,
    MatchAny,
    PointIdsList,
    PointStruct,
    VectorParams,
)
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.vector_db_handler.vector_db_base_handler import (
    BaseVectorDBHandler,
)

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

load_dotenv()  # Load environment variables from .env file


class QdrantManager(BaseVectorDBHandler):
    def __init__(self, vector_db_params: dict, index_params: dict):
        super().__init__(vector_db_params, index_params)
        self.collection_name = index_params["collection_name"]
        url = vector_db_params["url"]
        if "https" in url:
            self.client = QdrantClient(
                url=url, api_key=os.getenv("QDRANT_API_KEY"), timeout=60.0
            )
        else:
            self.client = QdrantClient(url=url, timeout=60.0)
        self.vector_size = index_params["vector_size"]
        self.distance_metric = index_params["distance_metric"]

    def check_if_index_exists(self) -> bool:
        result = False
        try:
            # Check if collection exists
            if self.client.get_collection(self.collection_name):
                result = True
        except Exception as e:
            logger.info(
                f"Collection {self.collection_name} does not exist. Creating now!"
            )
        return result

    def create_index(self):
        # Create a new collection
        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=self.vector_size,  # Adjust based on your embedding model
                distance=getattr(
                    Distance, self.distance_metric
                ),  # Distance metric (COSINE, EUCLID, etc.)
            ),
        )
        logger.info(f"Collection {self.collection_name} created")

    def insert_vector(self, vector: List[float], payload: Dict):
        point_id = payload.get("chunk_id") or payload.get("point_id")
        logger.info("Inserting vector with ID: " + str(point_id))
        if point_id is None:
            raise ValueError("Payload must include an 'id' field for upserting.")

        point = PointStruct(id=point_id, vector=vector, payload=payload)
        self.client.upsert(collection_name=self.collection_name, points=[point])

    def insert_vectors(self, vectors_payloads: List[Dict[str, Any]]):
        points = [
            PointStruct(id=payload["chunk_id"], vector=vector, payload=payload)
            for vector, payload in vectors_payloads
        ]
        self.client.upsert(collection_name=self.collection_name, points=points)

    def search_vectors(
        self, query_vector: List[float], limit: int = 1, body: Dict[str, Any] = None
    ):
        # Convert tensor to list if necessary
        if hasattr(query_vector, "tolist"):
            query_vector = query_vector.tolist()

        # Type-check: Must be a list.
        if not isinstance(query_vector, list):
            raise ValueError("query_vector must be a one-dimensional list of floats.")

        results = self.client.search(
            collection_name=self.collection_name, query_vector=query_vector, limit=limit
        )
        search_results = []
        for cur_point in results:
            op_dic = {}
            op_dic["id"] = cur_point.id
            op_dic["score"] = cur_point.score
            op_dic["payload"] = cur_point.payload
            search_results.append(op_dic)

        return search_results

    def fetch_points_by_payload(
        self, payload_filter: Dict[str, Any], limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch points from the collection based on matching payload fields.

        Args:
            payload_filter (Dict[str, Any]): The filter criteria for payload fields in key-value format.
            limit (int): Maximum number of points to retrieve per batch.

        Returns:
            List[Dict[str, Any]]: List of matching points with their IDs and payloads.
        """
        filter_criteria = []
        for key, value in payload_filter.items():
            if isinstance(value, list):
                # Use MatchAny for list-based values, so any item in the list can match
                filter_criteria.append(
                    FieldCondition(key=key, match=MatchAny(any=value))
                )
            else:
                # Use MatchValue for single-value fields
                filter_criteria.append(
                    FieldCondition(key=key, match=MatchValue(value=value))
                )

        # Create the filter object
        qdrant_filter = Filter(must=filter_criteria)

        # Initialize variables for pagination
        offset = 0
        all_points = []

        while True:
            # Perform a scroll request to retrieve points with the specified filter and limit
            scroll_result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=qdrant_filter,
                offset=offset,
                limit=limit,
            )

            # Extract points and the next offset
            points, next_offset = scroll_result

            # Add retrieved points to the result list
            all_points.extend(
                [{"id": point.id, "payload": point.payload} for point in points]
            )

            # If there's no next_offset, we have reached the end of the collection
            if not next_offset:
                break

            # Update offset for the next batch
            offset = next_offset

        return all_points

    def delete_index(self):
        self.client.delete_collection(collection_name=self.collection_name)

    def delete_points_by_key(self, key: str, value: str):
        """
        Deletes points from the Qdrant collection where the specified `field_name` in the payload
        matches any value in the provided list of `values`.

        Args:
            field_name (str): The payload field name to filter points by.
            values (list[str]): List of values to match for deletion.
        """
        if not key:
            print("No field name provided for deletion.")
            return
        if not value:
            print("No values provided for deletion.")
            return
        filter_query = Filter(
            must=[FieldCondition(key=key, match=MatchValue(value=value))]
        )

        # Retrieve Points(Chunks) that match the filter:
        scroll_result = self.client.scroll(
            collection_name=self.collection_name, scroll_filter=filter_query, limit=1000
        )

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
                    must=[
                        FieldCondition(
                            key=key, match=MatchExcept(**{"except": ["None"]})
                        )
                    ],
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


def main():
    # Initialize the QdrantManager with test parameters
    url = "http://localhost:6333"
    collection_name = "articles_metadata"
    vector_size = 768  # Set this according to your model's vector size

    # Create QdrantManager instance
    qdrant_manager = QdrantManager(
        url=url,
        collection_name=collection_name,
        vector_size=vector_size,
        distance_metric="COSINE",
    )

    # Step 1: Create Collection
    # if not qdrant_manager.check_if_collection_exists():
    #     qdrant_manager.create_collection()
    # print(f"Collection '{collection_name}' is ready.")
    #
    # # Step 2: Generate and Insert a Test Vector with Payload
    # test_vector = np.random.rand(vector_size).tolist()  # Random vector for testing
    # test_payload = {
    #     "chunk_id": str(uuid.uuid4()),
    #     "text": "This is a sample text for testing.",
    #     "metadata": {"source": "test", "type": "test_chunk"},
    # }
    #
    # qdrant_manager.insert_vector(vector=test_vector, payload=test_payload)
    # print("Test vector inserted successfully.")
    #
    # # Step 3: Perform a Search
    # search_results = qdrant_manager.search_vectors(query_vector=test_vector, limit=3)
    # print("Search results:")
    # for result in search_results:
    #     print(f"ID: {result.id}, Score: {result.score}, Payload: {result.payload}")
    #
    # # Step 4: Delete Collection
    # # qdrant_manager.delete_collection()
    # print(f"Collection '{collection_name}' deleted.")

    # # Step 5: Fetch Points using Payload Filter
    payload_filter = {
        "journal": "Nature",
        # "year": "2023"
    }
    points = qdrant_manager.fetch_points_by_payload(payload_filter, limit=5000)
    print(points)


if __name__ == "__main__":
    main()
