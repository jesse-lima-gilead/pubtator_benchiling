import json

from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.vector_db_handler.vector_db_handler_factory import (
    VectorDBHandler,
)

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class EmbeddingsLoader:
    def __init__(
        self,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        vector_db_type: str = None,
        vector_db_params: dict = None,
        index_params: dict = None,
    ):
        self.embeddings_dir = paths_config["embeddings_path"]
        self.file_handler = file_handler
        self.vector_db_type = vector_db_type
        # Get the Vector DB Handler with for specific vector db config
        try:
            vector_db_handler = VectorDBHandler(
                vector_db_params=vector_db_params, index_params=index_params
            )
            self.vector_db_manager = vector_db_handler.get_vector_db_manager(
                vector_db_type=vector_db_type
            )
        except Exception as e:
            logger.error(f"Error initializing vector database handler: {e}")
            raise

    def load_embeddings(self):
        for file in self.file_handler.list_files(self.embeddings_dir):
            logger.info(f"Loading {file} into {self.vector_db_type}")
            embeddings_file_path = self.file_handler.get_file_path(
                self.embeddings_dir, file
            )

            try:
                embeddings_file_data = self.file_handler.read_json_file(
                    embeddings_file_path
                )
                if not isinstance(embeddings_file_data, list):
                    logger.warning(
                        f"Unexpected data format in {file}. Expected list of vectors."
                    )
                    continue

                self.vector_db_manager.insert_vectors(embeddings_file_data)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON in file: {file}")
            except Exception as e:
                logger.error(f"Error processing file {file}: {e}")


def run(collection_type: str = "processed_pubmedbert"):
    try:
        # Initialize the config loader
        config_loader = YAMLConfigLoader()

        # Retrieve paths config
        paths_config = config_loader.get_config("paths")
        if not paths_config:
            raise ValueError("Paths configuration is missing.")

        storage_type = paths_config["storage"].get("type")
        if not storage_type:
            raise ValueError("Storage type is missing in paths configuration.")

        # Get file handler instance from factory
        file_handler = FileHandlerFactory.get_handler(storage_type)
        paths = paths_config["storage"].get(storage_type)
        if not paths:
            raise ValueError(f"No storage paths found for type: {storage_type}")

        # Retrieve vector DB specific config
        vectordb_config = config_loader.get_config("vectordb")["vector_db"]
        vector_db_type = vectordb_config.get("type")
        if not vector_db_type:
            raise ValueError("Vector database type is missing in configuration.")

        vector_db_params = vectordb_config.get(vector_db_type, {}).get(
            "vector_db_params", {}
        )
        index_params = (
            vectordb_config.get(vector_db_type, {})
            .get("index_params", {})
            .get(collection_type)
        )
        if index_params is None:
            raise ValueError(
                f"Index parameters missing for collection type: {collection_type}"
            )

        embeddings_loader = EmbeddingsLoader(
            file_handler=file_handler,
            paths_config=paths,
            vector_db_type=vector_db_type,
            vector_db_params=vector_db_params,
            index_params=index_params,
        )

        embeddings_loader.load_embeddings()
    except Exception as e:
        logger.error(f"Error in run function: {e}")
        raise


if __name__ == "__main__":
    # Processed Text Collection
    # collection_type = "processed_pubmedbert"

    # Baseline Text Collection
    # collection_type = "baseline"

    # Test Collection
    collection_type = "test"

    run(collection_type=collection_type)
