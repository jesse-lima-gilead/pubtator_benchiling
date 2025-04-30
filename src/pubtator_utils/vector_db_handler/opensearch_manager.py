import os
from typing import Dict, List, Any
import boto3
from requests_aws4auth import AWS4Auth
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
            if "https" in host:
                # AWS credentials and region
                region = vector_db_params.get("region")
                service = vector_db_params.get("service")
                credentials = boto3.Session().get_credentials()
                auth = AWS4Auth(
                    credentials.access_key,
                    credentials.secret_key,
                    region,
                    service,
                    session_token=credentials.token,
                )
                host_details = [host]
            else:
                port = vector_db_params.get("port", 9200)
                username = os.getenv("OPENSEARCH_USER")
                password = os.getenv("OPENSEARCH_PASS")
                auth = (username, password) if username and password else None
                host_details = [{"host": host, "port": port}]

            use_ssl = vector_db_params.get("use_ssl", True)
            verify_certs = eval(vector_db_params.get("verify_certs", "False"))

            self.client = OpenSearch(
                hosts=host_details,
                http_auth=auth,
                use_ssl=use_ssl,
                verify_certs=verify_certs,
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

    def bulk_insert(self, vectors_payloads: List[Dict[str, Any]]):
        """
        Bulk indexes multiple vector documents.
        Each element in vectors_payloads is a tuple (vector, payload).
        """
        actions = []
        try:
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
            logger.error(f"Bulk insert failed: {e}")
            logger.info("Falling back to inserting documents one by one...")
            for action in actions:
                doc_id = action.get("_id")
                doc = action.get("_source")
                try:
                    self.client.index(index=self.index_name, id=doc_id, body=doc)
                except Exception as inner_e:
                    logger.error(
                        f"Failed to insert single document {doc_id}: {inner_e}"
                    )
                    raise
            logger.info(f"Inserted {len(actions)} vectors.")

    def search_vectors(
        self, query_vector: List[float], limit: int = 10, body: Dict[str, Any] = None
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
        self,
        query_vector: List[float],
        top_k: int,
        filters: Dict[str, Any],
        SCORE_THRESHOLD: float = 0.7,
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

        # New Date Filters
        if date := filters.get("date"):  # Format: YYYY-MM-DD
            year, month, day = date.split("-")
            filter_conditions.extend(
                [
                    {"term": {"publication_date.year": year}},
                    {
                        "term": {"publication_date.month": month.lstrip("0")}
                    },  # Remove leading zero
                    {
                        "term": {"publication_date.day": day.lstrip("0")}
                    },  # Remove leading zero
                ]
            )

        if month_year := filters.get("month_year"):  # Format: YYYY-MM
            year, month = month_year.split("-")
            filter_conditions.extend(
                [
                    {"term": {"publication_date.year": year}},
                    {
                        "term": {"publication_date.month": month.lstrip("0")}
                    },  # Remove leading zero
                ]
            )

        # Fuzzy match for title in OpenSearch
        if title := filters.get("title"):
            filter_conditions.append(
                {"match": {"title": {"query": title, "fuzziness": "AUTO"}}}
            )

        # # More stricter one
        # # Handle user_keywords filter:
        # if user_keywords := filters.get("user_keywords"):
        #     # Split by comma and trim whitespace
        #     keywords_list = [kw.strip() for kw in user_keywords.split(",")]
        #     # Create a bool query that matches if any one of the keywords matches
        #     user_keywords_filter = {
        #         "bool": {
        #             "should": [
        #                 {"match_phrase": {"chunk_text": kw}} for kw in keywords_list
        #             ],
        #             "minimum_should_match": 1
        #         }
        #     }
        #     filter_conditions.append(user_keywords_filter)

        # Slightly less stricter, allows for partial matches
        # Handle user_keywords filter with partial matching using a "match" query:
        if user_keywords := filters.get("user_keywords"):
            # Split by comma and trim whitespace
            keywords_list = [kw.strip() for kw in user_keywords.split(",")]
            # Create a bool query that matches if any one of the keywords partially matches the field
            user_keywords_filter = {
                "bool": {
                    "should": [
                        {"match": {"chunk_text": {"query": kw, "operator": "and"}}}
                        for kw in keywords_list
                    ],
                    "minimum_should_match": 1,
                }
            }
            filter_conditions.append(user_keywords_filter)

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
                "doi",
                "pmid",
                "title",
                "journal",
                "article_type",
                "publication_date",
                "authors",
                "merged_text",
                "vector",
            ],
        }

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

        # Similarity Threshold
        filtered_results = [res for res in results if res["score"] >= SCORE_THRESHOLD]

        # Return top_k results after post-filtering
        return filtered_results[:top_k]

    def get_distinct_values(
        self, field_name: str, field_value: str = None
    ) -> List[Dict[str, Any]]:
        """Retrieve unique values for a given filter, sorted by count."""

        if field_name in ["year", "publication_date.year"]:
            field_path = "publication_date.year.keyword"
        elif field_name in ["month", "publication_date.month"]:
            field_path = "publication_date.month.keyword"
        elif field_name in ["day", "publication_date.day"]:
            field_path = "publication_date.day.keyword"
        else:
            field_path = f"{field_name}.keyword"

        if field_value is not None:
            # If a field_value is provided, use a filter aggregation to count only that value.
            query = {
                "size": 0,
                "aggs": {
                    "filtered_docs": {
                        "filter": {"term": {field_path: field_value}},
                        "aggs": {
                            "article_count": {
                                "cardinality": {"field": "article_id.keyword"}
                            }
                        },
                    }
                },
            }
            response = self.client.search(index=self.index_name, body=query)
            count = response["aggregations"]["filtered_docs"]["article_count"]["value"]
            return [{field_name: field_value, "articles_count": count}]
        else:
            # No field_value provided: aggregate over all distinct values.
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

    def delete_document_by_id(self, doc_id):
        """
        Deletes a single document by its ID.
        """
        try:
            self.client.delete(index=self.index_name, id=doc_id)
            logger.info(f"Document {doc_id} deleted successfully.")
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
            raise

    def delete_documents_by_query(self, query):
        """
        Deletes all documents that match the given query.
        """
        try:
            self.client.delete_by_query(index=self.index_name, body={"query": query})
            logger.info(f"Documents deleted successfully.")
        except Exception as e:
            logger.error(f"Error deleting documents: {e}")
            raise

    def get_index_fields(self):
        """
        Fetch and print all fields of the index.
        """
        try:
            mapping = self.client.indices.get_mapping(index=self.index_name)
            properties = mapping[self.index_name]["mappings"]["properties"]
            print("Fields in Index:", self.index_name)
            for field, details in properties.items():
                print(f"- {field} (Type: {details.get('type', 'unknown')})")
        except Exception as e:
            print(f"Error retrieving index fields: {e}")

    def count_vectors(self) -> int:
        """
        Returns the total number of vector points stored in the OpenSearch index.
        """
        try:
            response = self.client.count(index=self.index_name)
            return response["count"]
        except Exception as e:
            logger.error(f"Error counting vectors in index {self.index_name}: {e}")
            return 0

    def get_index_mapping(self):
        try:
            mapping = self.client.indices.get_mapping(index=self.index_name)
            properties = mapping[self.index_name]["mappings"]["properties"]
            return properties
        except Exception as e:
            print(f"Error getting mapping: {e}")
            raise
