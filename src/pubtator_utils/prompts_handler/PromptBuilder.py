# prompt_builder.py
from typing import List

from src.pubtator_utils.prompts_handler.guidelines import (
    ARTICLE_SUMMARY_GUIDELINES,
    LLM_RESPONSE_GUIDELINES,
)
from src.pubtator_utils.prompts_handler.instructions import (
    ARTICLE_SUMMARY_INSTRUCTIONS,
    LLM_RESPONSE_INSTRUCTIONS,
)
from src.pubtator_utils.prompts_handler.persona import (
    ARTICLE_SUMMARY_PERSONA,
    LLM_RESPONSE_PERSONA,
)
from src.pubtator_utils.prompts_handler.output_format import LLM_RESPONSE_OUTPUT_FORMAT


class PromptBuilder:
    def __init__(self):
        self.article_summary_persona = ARTICLE_SUMMARY_PERSONA
        self.article_summary_instructions = ARTICLE_SUMMARY_INSTRUCTIONS
        self.article_summary_guidelines = ARTICLE_SUMMARY_GUIDELINES
        self.llm_response_persona = LLM_RESPONSE_PERSONA
        self.llm_response_guidelines = LLM_RESPONSE_GUIDELINES
        self.llm_response_instructions = LLM_RESPONSE_INSTRUCTIONS
        self.llm_response_output_format = LLM_RESPONSE_OUTPUT_FORMAT

    def get_article_summary_combined_prompt(self, pmc_article_text) -> str:
        combined_prompt = (
            f"{self.article_summary_persona}\n\n"
            f"Instruction:\n {self.article_summary_instructions}\n\n"
            f"Guidelines:\n {self.article_summary_guidelines}\n\n"
            f"PMC Article:\n {pmc_article_text}\n\n"
            f"The Output Format should STRICTLY be JSON.\n"
        )
        return combined_prompt

    def get_llm_response_prompt(
        self, user_query: str, relevant_chunks: List[str], article_id: str
    ):
        combined_prompt = (
            f"{self.llm_response_persona}\n\n"
            f"Instruction:\n {self.llm_response_instructions}\n\n"
            f"Guidelines:\n {self.llm_response_guidelines}\n\n"
            f"User: {user_query}\n\n"
            f"Relevant Content:\n\n"
            f"{' '.join(relevant_chunks)}\n\n"
            f"Article ID: {article_id}\n\n"
            f"Output Format:{self.llm_response_output_format}\n"
        )
        return combined_prompt
