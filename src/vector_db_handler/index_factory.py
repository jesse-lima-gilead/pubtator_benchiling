from vector_db_handler.qdrant_handler import QdrantHandler
from utils.config_reader import YAMLConfigLoader

# Initialize the config loader
config_loader = YAMLConfigLoader()


class IndexFactory:
    def __init__(
        self,
        chunker_type: str,
        merger_type: str,
        ner_model: str,
        llm_model: str = "BedrockClaude",
    ):
        self.chunker_type = chunker_type
        self.merger_type = merger_type
        self.ner_model = ner_model
        self.vector_db_configs = config_loader.get_config("vector_db")
        self.llm_model = llm_model

    def get_indexer(self, vector_db):
        """Factory method to return the appropriate indexer based on the vector_db."""
        if vector_db == "qdrant":
            return QdrantHandler(
                self.chunker_type,
                self.merger_type,
                self.ner_model,
                self.vector_db_configs["qdrant"],
                self.llm_model,
            )
        else:
            raise ValueError(f"Unknown Vector DB: '{vector_db}'")
