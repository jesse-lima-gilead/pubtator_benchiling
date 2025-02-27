from src.pubtator_utils.vector_db_handler.qdrant_manager import QdrantManager
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def create_collections(qdrant_db_config):
    """Method to create collection in qdrant database."""
    host = qdrant_db_config["host"]
    port = qdrant_db_config["port"]
    collections = qdrant_db_config["collections"]

    for chunker_type in collections:
        for merger_type in collections[chunker_type]:
            for model_used in collections[chunker_type][merger_type]:
                params_dic = collections[chunker_type][merger_type][model_used]
                collection_name = params_dic["collection_name"]
                vector_size = params_dic["vector_size"]
                distance_metric = params_dic["distance_metric"]

                qdm_manager = QdrantManager(
                    host, port, collection_name, vector_size, distance_metric
                )
                qdm_manager.delete_collection()
                if not qdm_manager.check_if_collection_exists():
                    qdm_manager.create_collection()


# Run the main function
if __name__ == "__main__":
    logger.info(f"Creation of Qdrant Collections Execution Started")
    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve a specific config
    qdrant_db_config = config_loader.get_config("vector_db")["qdrant"]
    create_collections(qdrant_db_config)
