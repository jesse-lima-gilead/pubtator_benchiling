from src.pubtator_utils.llm_handler.bedrock_handler import BedrockHandler


class LLMFactory:
    def create_llm(self, llm_type: str, **kwargs):
        if llm_type == "BedrockClaude":
            default_llm_params = {
                "max_tokens": 50000,
                "temperature": 0.1,
                "top_p": 0.9,
                "top_k": 250,
                "credentials_profile_name": "ishaan",
                "query_llm_model_id": "anthropic.claude-v2",
                "embed_llm_model_id": "amazon.titan-embed-text-v1",
            }
            default_llm_params.update(kwargs)
            return BedrockHandler(**kwargs)

        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")


# Example usage of the factory:
# factory = LLMFactory()
# llm = factory.create_llm("Claude", model_kwargs={"temperature": 0.1})
