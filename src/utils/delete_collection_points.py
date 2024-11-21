from src.vector_db_handler.qdrant_handler import QdrantHandler
from src.utils.config_reader import YAMLConfigLoader
from src.utils.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
vectordb_config = config_loader.get_config("vectordb")["qdrant"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def initialize_article_qdrant_manager(embeddings_collection_type: str):
    # Initialize the QdrantHandler
    qdrant_handler = QdrantHandler(
        collection_type=embeddings_collection_type, params=vectordb_config
    )
    qdrant_manager = qdrant_handler.get_qdrant_manager()
    return qdrant_manager


def run():
    collection_type = "baseline"
    qdrant_manager = initialize_article_qdrant_manager(
        embeddings_collection_type=collection_type
    )
    logger.info(
        f"Qdrant Manager initialized successfully for collection: {qdrant_manager.collection_name}"
    )
    extra_articles_id = [
        "9413286",
        "7444693",
        "7442721",
        "7737765",
        "9050543",
        "8748704",
        "9167747",
        "9161072",
        "9746914",
        "9674284",
        "9896310",
        "11332722",
        "10117631",
        "10017705",
        "10232659",
        "10443631",
        "10698546",
        "10870877",
        "10645594",
        "10831337",
        "10912034",
        "10907391",
        "10837166",
        "10897627",
        "10915134",
        "10923916",
        "11208295",
        "11319832",
        "11008188",
        "11014662",
        "11116757",
        "11118283",
        "11231252",
        "11245995",
        "11371094",
        "11405273",
        "11387199",
        "8418271",
        "8579308",
        "8784611",
        "10119142",
        "10370087",
        "10619435",
        "11073880",
        "8698540",
    ]
    key = "article_id"

    for article_id in extra_articles_id:
        value = f"PMC_{article_id}"
        qdrant_manager.delete_points_by_key(key=key, value=value)
        logger.info(
            f"Deleted article {article_id} from Qdrant Collection: {qdrant_manager.collection_name}"
        )


if __name__ == "__main__":
    run()
