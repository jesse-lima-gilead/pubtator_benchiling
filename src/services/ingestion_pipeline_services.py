from src.data_ingestion.articles_ingestor import PMCIngestor
from src.utils.config_handler.config_reader import YAMLConfigLoader
from src.utils.logs_handler.logger import SingletonLogger
from src.utils.file_handler import FileHandlerFactory
from src.pydantic_models.ingestion_models import (
    PMC_Articles_Extractor_Request,
    Metadata_Extractor_Request,
)

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()


def _get_pmc_ingestor():
    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]
    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    pmc_ingestor = PMCIngestor(
        file_handler=file_handler,
        paths_config=paths,
    )
    return pmc_ingestor


def pmc_articles_extractor_service(request: PMC_Articles_Extractor_Request):
    logger.info("Execution Started")

    query = request.query
    start_date = request.start_date
    end_date = request.end_date
    retmax = request.ret_max

    # Retrieve dataset config
    dataset_config = config_loader.get_config("curated_dataset")
    article_ids = (
        dataset_config["golden_dataset_article_ids"]
        + dataset_config["enhanced_golden_dataset_article_ids"]
        + dataset_config["litqa_dataset_article_ids"]
        + dataset_config["poc_dataset_article_ids"]
    )

    # # Sample article Ids for testing
    # sample_articles_id = ["2361529", "2480972"]

    pmc_ingestor = _get_pmc_ingestor()

    extracted_articles_count = pmc_ingestor.pmc_articles_extractor(
        article_ids=article_ids,
        query=query,
        start_date=start_date,
        end_date=end_date,
        retmax=retmax,
    )
    logger.info("Execution Completed")

    return {
        "message": f"{extracted_articles_count} PMC Articles Extracted Successfully!"
    }


def articles_metadata_extractor_service(request: Metadata_Extractor_Request):
    metadata_storage_type = request.storage_type
    pmc_ingestor = _get_pmc_ingestor()
    pmc_ingestor.articles_metadata_extractor(metadata_storage_type)
