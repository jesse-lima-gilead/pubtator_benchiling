import os
from typing import Dict, List, Any
from rapidfuzz import fuzz
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
            for vector_payload in vectors_payloads:
                vector = vector_payload["embeddings"]
                payload = vector_payload["payload"]
                point_id = vector_payload["payload"]["chunk_id"]
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

    def fuzzy_match(self, query: str, match_list: List[str], threshold: int = 70):
        """
        Performs fuzzy matching for the given author against a list of authors.
        Returns True if a match is found with a similarity above the threshold.
        """
        for entry in match_list:
            if fuzz.ratio(query.lower(), entry.lower()) >= threshold:
                return True
        return False

    def search_with_filters(
        self, query_vector: List[float], top_k: int, filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Search with pre-filtering and post-filtering in OpenSearch"""

        # Pre-Filtering (Low Cardinality Fields)
        filter_conditions = []

        if journal := filters.get("journal"):
            filter_conditions.append({"term": {"journal.keyword": journal}})

        if article_type := filters.get("article_type"):
            filter_conditions.append({"term": {"article_type.keyword": article_type}})

        if year := filters.get("year"):
            filter_conditions.append({"term": {"publication_date.year": year}})

        if years_after := filters.get("years_after"):
            filter_conditions.append(
                {"range": {"publication_date.year": {"gte": years_after}}}
            )

        if years_before := filters.get("years_before"):
            filter_conditions.append(
                {"range": {"publication_date.year": {"lte": years_before}}}
            )

        # Construct Query
        base_query = {
            "size": top_k * 2,
            "query": {
                "bool": {
                    "must": [
                        {"knn": {"vector": {"vector": query_vector, "k": top_k * 2}}}
                    ],
                    "filter": filter_conditions,
                }
            },
            "_source": [
                "article_id",
                "title",
                "journal",
                "publication_date",
                "authors",
                "chunk_text",
                "vector",
            ],
        }

        # Fuzzy match for title in OpenSearch
        if title := filters.get("title"):
            base_query["query"]["bool"].setdefault("must", []).append(
                {"match": {"title": {"query": title, "fuzziness": "AUTO"}}}
            )

        # Execute OpenSearch Query
        response = self.client.search(index=self.index_name, body=base_query)

        # Convert response to list of results
        results = [
            {
                "id": hit["_id"],
                "score": hit["_score"],
                "metadata": {k: v for k, v in hit["_source"].items() if k != "vector"},
            }
            for hit in response["hits"]["hits"]
        ]

        # Post-Filtering (High Cardinality: Authors)
        if authors := filters.get("authors"):
            logger.info("Inside post-filtering")
            results = [
                res
                for res in results
                if self.fuzzy_match(
                    query=authors, match_list=res["metadata"].get("authors", [])
                )
            ]

        # Return top_k results after post-filtering
        return results[:top_k]

    def get_distinct_values(self, field_name: str) -> List[Dict[str, Any]]:
        """Retrieve unique values for a given filter, sorted by count."""
        field_path = (
            f"{field_name}.keyword"
            if field_name not in ["year", "publication_date.year"]
            else "publication_date.year.keyword"
        )

        # query = {
        #     "size": 0,
        #     "aggs": {
        #         "distinct_values": {
        #             "terms": {"field": field_path, "size": 1000, "order": {"article_count": "desc"}},
        #             "aggs": {
        #                 "article_count": {"cardinality": {"field": "article_id.keyword"}}
        #             }
        #         }
        #     }
        # }

        query = {
            "size": 0,
            "aggs": {
                "distinct_values": {
                    "terms": {"field": field_path, "size": 1000},
                    "aggs": {
                        "article_count": {
                            "cardinality": {"field": "article_id.keyword"}
                        }
                    },
                }
            },
        }

        response = self.client.search(index=self.index_name, body=query)

        # return [
        #     {"value": bucket["key"], "count": bucket["doc_count"]}
        #     for bucket in response["aggregations"]["distinct_values"]["buckets"]
        # ]

        return sorted(
            [
                {
                    f"{field_name}": bucket["key"],
                    "articles_count": bucket["article_count"]["value"],
                }
                for bucket in response["aggregations"]["distinct_values"]["buckets"]
            ],
            key=lambda x: x["articles_count"],
            reverse=True,
        )

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
