# prompt_builder.py
from src.Prompts.guidelines import GUIDELINES, IMAGE_DESC_GUIDELINES, OCR_GUIDELINES
from src.Prompts.instructions import (
    IMAGE_DESC_INSTRUCTIONS,
    INSTRUCTIONS,
    OCR_INSTRUCTIONS,
)
from src.Prompts.persona import IMAGE_DESC_PERSONA, PERSONA, OCR_PERSONA


class PromptBuilder:
    def __init__(self):
        self.ocr_guidelines = OCR_GUIDELINES
        self.ocr_instructions = OCR_INSTRUCTIONS
        self.ocr_persona = OCR_PERSONA
        self.persona = PERSONA
        self.instructions = INSTRUCTIONS
        self.guidelines = GUIDELINES

        self.image_desc_persona = IMAGE_DESC_PERSONA
        self.image_desc_instructions = IMAGE_DESC_INSTRUCTIONS
        self.image_desc_guidelines = IMAGE_DESC_GUIDELINES

    def get_combined_prompt(self, user_query) -> str:
        combined_prompt = (
            f"{self.persona}\n\n"
            f"{self.instructions}\n\n"
            f"{self.guidelines}\n\n"
            f"User: {user_query}\n"
            "Assistant:"
        )
        return combined_prompt

    def get_image_desc_prompt(self) -> str:
        image_desc_prompt = (
            f"{self.image_desc_persona}\n\n"
            f"{self.image_desc_instructions}\n\n"
            f"{self.image_desc_guidelines}\n\n"
        )
        return image_desc_prompt

    def get_combined_image_text_prompt(self, user_query) -> str:
        combined_prompt = (
            f"{self.ocr_persona}\n\n"
            f"{self.ocr_instructions}\n\n"
            f"{self.ocr_guidelines}\n\n"
            f"User: {user_query}\n"
            "Assistant:"
        )
        return combined_prompt
