# prompt_builder.py
from src.Prompts.guidelines import ARTICLE_SUMMARY_GUIDELINES
from src.Prompts.instructions import ARTICLE_SUMMARY_INSTRUCTIONS
from src.Prompts.persona import ARTICLE_SUMMARY_PERSONA


class PromptBuilder:
    def __init__(self):
        self.article_summary_persona = ARTICLE_SUMMARY_PERSONA
        self.article_summary_instructions = ARTICLE_SUMMARY_INSTRUCTIONS
        self.article_summary_guidelines = ARTICLE_SUMMARY_GUIDELINES

    def get_article_summary_combined_prompt(self, pmc_article_text) -> str:
        combined_prompt = (
            f"{self.article_summary_persona}\n\n"
            f"Instruction:\n {self.article_summary_instructions}\n\n"
            f"Additional guidelines:\n {self.article_summary_guidelines}\n\n"
            f"PMC Article:\n {pmc_article_text}\n\n"
            "Provide ONLY the Summary text, without any introductory phrases, meta-commentary, or concluding remarks."
        )
        return combined_prompt
