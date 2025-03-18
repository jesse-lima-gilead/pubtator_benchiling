from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pydantic_models.retrieval_models import SearchRequest, ValuesSearchRequest
from src.data_retrieval.pubtator_retriever import search, get_distinct_values

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()


def pmc_articles_search_service(request: SearchRequest):
    user_query = request.user_query
    metadata_filter = request.metadata_filters
    chunks_by_article = search(user_query=user_query, metadata_filters=metadata_filter)
    return chunks_by_article


def pmc_metadata_values_service(request: ValuesSearchRequest):
    field_name = request.field_name
    distinct_values = get_distinct_values(field_name=field_name)
    return distinct_values
