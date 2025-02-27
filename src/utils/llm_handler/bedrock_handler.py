from langchain_aws import ChatBedrock
from langchain_aws.embeddings import BedrockEmbeddings

from src.utils.aws_handler.aws_connect import AWSConnection


class BedrockHandler:
    def __init__(self, **kwargs):
        self.max_tokens = kwargs.get("max_tokens", 50000)
        self.temperature = kwargs.get("temperature", 0.15)
        self.top_p = kwargs.get("top_p", 0.95)
        self.top_k = kwargs.get("top_k", 250)
        self.bedrock_client = AWSConnection().get_client("bedrock-runtime")
        self.query_llm_model_id = kwargs.get(
            "query_llm_model_id", "anthropic.claude-v2:1"
        )
        self.multi_modal_vision_model_id = kwargs.get(
            "multi_modal_vision_model_id", "anthropic.claude-3-5-sonnet-20240620-v1:0"
        )
        self.embed_llm_model_id = kwargs.get(
            "embed_llm_model_id", "amazon.titan-embed-text-v1"
        )

    def get_query_llm(self):
        model_kwargs = {
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "top_k": self.top_k,
        }
        llm = ChatBedrock(
            client=self.bedrock_client,
            model_id=self.query_llm_model_id,
            model_kwargs=model_kwargs,
        )
        return llm

    def get_embed_llm(self):
        # Get the bedrock client
        embeddings_llm = BedrockEmbeddings(
            client=self.bedrock_client, model_id=self.embed_llm_model_id
        )
        return embeddings_llm
