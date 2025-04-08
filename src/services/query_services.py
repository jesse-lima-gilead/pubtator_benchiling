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
    top_n = request.top_n
    top_k = request.top_k
    score_threshold = request.score_threshold
    embeddings_model = request.embeddings_model
    show_as_table = request.show_as_table
    chunks_by_article = search(
        user_query=user_query,
        metadata_filters=metadata_filter,
        top_n=top_n,
        top_k=top_k,
        score_threshold=score_threshold,
        embeddings_model=embeddings_model,
        show_as_table=show_as_table,
    )
    return chunks_by_article


def pmc_metadata_values_service(request: ValuesSearchRequest):
    field_name = request.field_name
    field_value = request.field_value
    show_as_table = request.show_as_table
    distinct_values = get_distinct_values(
        field_name=field_name,
        field_value=field_value,
        show_as_table=show_as_table,
    )
    return distinct_values
