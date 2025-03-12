import os
from typing import Dict, List, Any

from dotenv import load_dotenv
from opensearchpy import OpenSearch, exceptions, helpers, RequestsHttpConnection
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.vector_db_handler.vector_db_base_handler import (
    BaseVectorDBHandler,
)

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

load_dotenv()  # Load environment variables from .env file


class OpenSearchManager(BaseVectorDBHandler):
    def __init__(self, vector_db_params: dict, index_params: dict):
        super().__init__(vector_db_params, index_params)
        try:
            self.index_name = index_params["index_name"]
            self.vector_size = index_params["vector_size"]
            self.space_type = index_params.get("space_type", "l2")
            self.engine = index_params.get("engine", "nmslib")
            self.method = index_params.get("method", "hnsw")

            host = vector_db_params["host"]
            port = vector_db_params.get("port", 9200)
            username = os.getenv("OPENSEARCH_USER")
            password = os.getenv("OPENSEARCH_PASS")
            auth = (username, password) if username and password else None
            use_ssl = vector_db_params.get("use_ssl", True)

            self.client = OpenSearch(
                hosts=[{"host": host, "port": port}],
                http_auth=auth,
                use_ssl=use_ssl,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
                connection_class=RequestsHttpConnection,
            )
        except KeyError as e:
            raise ValueError(f"Missing required parameter: {e}")
        except Exception as e:
            logger.error(f"Error initializing OpenSearch client: {e}")
            raise

    def check_if_index_exists(self) -> bool:
        try:
            return self.client.indices.exists(index=self.index_name)
        except Exception as e:
            logger.info(f"Error checking if index {self.index_name} exists: {e}")
            return False

    def create_index(self):
        """
        Creates an OpenSearch index with a mapping for a KNN vector field.
        """
        try:
            body = {
                "settings": {
                    "index": {
                        "knn": True,
                        "knn.algo_param.ef_search": 100,
                        "knn.algo_param.ef_construction": 100,
                        "knn.algo_param.m": 16,
                    }
                },
                "mappings": {
                    "properties": {
                        "vector": {
                            "type": "knn_vector",
                            "dimension": self.vector_size,
                            "method": {
                                "name": self.method,
                                "space_type": self.space_type,
                                "engine": self.engine,
                                "parameters": {
                                    "ef_construction": 100,
                                    "m": 16,
                                },
                            },
                        },
                        "payload": {"type": "object"},
                    }
                },
            }
            self.client.indices.create(index=self.index_name, body=body, ignore=400)
            logger.info(f"Index {self.index_name} created successfully.")
        except Exception as e:
            logger.error(f"Error creating index {self.index_name}: {e}")
            raise

    def insert_vector(self, vector: List[float], payload: Dict):
        """
        Inserts a single vector document into the OpenSearch index.
        Expects payload to contain either 'chunk_id' or 'point_id' as a unique identifier.
        """
        try:
            point_id = payload.get("chunk_id") or payload.get("point_id")
            if not point_id:
                raise ValueError(
                    "Payload must include 'chunk_id' or 'point_id' as a unique identifier."
                )

            if hasattr(vector, "tolist"):
                vector = vector.tolist()
            if not isinstance(vector, list) or not all(
                isinstance(x, float) for x in vector
            ):
                raise ValueError("vector must be a one-dimensional list of floats.")

            doc = payload.copy()
            doc["vector"] = vector

            self.client.index(
                index=self.index_name, id=point_id, body=doc, refresh=True
            )
            logger.info(f"Inserted vector with ID: {point_id}")
        except Exception as e:
            logger.error(f"Error inserting vector: {e}")
            raise

    # needs testing
    def insert_vectors(self, vectors_payloads: List[Dict[str, Any]]):
        """
        Bulk indexes multiple vector documents.
        Each element in vectors_payloads is a tuple (vector, payload).
        """
        try:
            actions = []
            for vector, payload in vectors_payloads:
                point_id = payload.get("chunk_id") or payload.get("point_id")
                if not point_id:
                    raise ValueError(
                        "Payload must include 'chunk_id' or 'point_id' as a unique identifier."
                    )

                if hasattr(vector, "tolist"):
                    vector = vector.tolist()
                if not isinstance(vector, list) or not all(
                    isinstance(x, float) for x in vector
                ):
                    raise ValueError(
                        "Each vector must be a one-dimensional list of floats."
                    )

                actions.append(
                    {
                        "_index": self.index_name,
                        "_id": point_id,
                        "_source": {**payload, "vector": vector},
                    }
                )

            helpers.bulk(self.client, actions, refresh=True)
            logger.info(f"Inserted {len(actions)} vectors.")
        except Exception as e:
            logger.error(f"Error inserting multiple vectors: {e}")
            raise

    def search_vectors(
        self, query_vector: List[float], limit: int = 1, body: Dict[str, Any] = None
    ):
        """
        Executes a KNN search query to find the nearest vectors.
        """
        # Convert tensor to list if necessary
        if hasattr(query_vector, "tolist"):
            query_vector = query_vector.tolist()

        # Type-check: Must be a list.
        if not isinstance(query_vector, list):
            raise ValueError("query_vector must be a one-dimensional list of floats.")

        # body = {
        #     "size": limit,
        #     "query": {
        #         "knn": {
        #             "vector": {
        #                 "vector": query_vector,
        #                 "k": limit,
        #             }
        #         }
        #     },
        # }
        response = self.client.search(index=self.index_name, body=body)
        results = response["hits"]["hits"]

        search_results = []
        for cur_doc in results:
            op_dic = {}
            op_dic["id"] = cur_doc["_id"]
            op_dic["score"] = cur_doc["_score"]
            del cur_doc["_source"]["vector"]
            op_dic["payload"] = cur_doc["_source"]
            search_results.append(op_dic)

        return search_results

    def delete_index(self):
        """
        Deletes the OpenSearch index.
        """
        try:
            self.client.indices.delete(index=self.index_name, ignore=[400, 404])
            logger.info(f"Index {self.index_name} deleted successfully.")
        except Exception as e:
            logger.error(f"Error deleting index {self.index_name}: {e}")
            raise
