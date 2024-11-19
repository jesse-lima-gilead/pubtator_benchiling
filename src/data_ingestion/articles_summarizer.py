import xml.etree.ElementTree as ET
from src.Prompts.PromptBuilder import PromptBuilder
from src.utils.logger import SingletonLogger
from src.llm_handler.llm_factory import LLMFactory
import os
import re

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class SummarizeArticle:
    """Class to load Pubmed Article's content and Summarize it."""

    def __init__(self, input_file_path: str):
        self.prompt_builder = PromptBuilder()
        self.input_file_path = input_file_path
        self.pmc_article_text = self._load_file_content()
        llm_factory = LLMFactory()
        llm_handler = llm_factory.create_llm(llm_type="BedrockClaude")
        self.query_llm = llm_handler.get_query_llm()

    def _load_file_content(self) -> str:
        """Parses the XML file and extracts relevant text content."""
        try:
            tree = ET.parse(self.input_file_path)
            root = tree.getroot()

            content = ""
            for passage in root.findall(".//passage"):
                passage_text = passage.findtext("text")
                if passage_text:
                    content += passage_text + "\n"
            return content.strip()

        except ET.ParseError as e:
            raise ValueError(f"Failed to parse XML: {e}")
        except FileNotFoundError:
            raise ValueError(f"File not found: {self.input_file_path}")

    def get_clean_summary(self, text: str):
        """
        Remove meta-commentary from the given summary text.

        Args:
            text (str): The summary text with possible meta-commentary.

        Returns:
            str: The cleaned summary text.
        """
        # Define patterns for meta-commentary (adjust as needed)
        patterns = [
            r"^Abstract .*?",
            r"^Summary .*?",
            r"^In summary .*?",
            r"^In conclusion .*?",
            r"^Here is a one-liner summary of the .*?",
            r"^Here is a concise summary of .*?"
            r"^The article discusses the following .*?"
            r"^The main findings from the article are as follows .*?"
            r"^The article can be summarized as follows .*?"
            r"^Summary of the file content .*?",
        ]

        # Remove any matching meta-commentary
        for pattern in patterns:
            clean_summary = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()

        return clean_summary

    def summarize(self):
        # Prepare the Prompt for the LLM
        custom_article_summary_prompt = (
            self.prompt_builder.get_article_summary_combined_prompt(
                self.pmc_article_text
            )
        )
        logger.info(f"Generated prompt: {custom_article_summary_prompt}")

        # Generate the response using Query LLM
        llm_summary_response = self.query_llm.invoke(
            input=custom_article_summary_prompt
        )

        # Parse the LLM response to fetch the summary
        response_content = llm_summary_response.content
        # clean_summary = self.get_clean_summary(response_content)
        response_lines = response_content.strip().splitlines()
        if len(response_lines) > 1:
            clean_summary = response_lines[-1]
        else:
            clean_summary = response_lines[0]

        logger.info(f"Generated summary: {clean_summary}")
        return clean_summary


# Usage
if __name__ == "__main__":
    input_file_path = "../../data/ner_processed/gnorm2_annotated/PMC_7614604.xml"
    summarizer = SummarizeArticle(input_file_path)
    summary = summarizer.summarize()
    print("Summary of the file content:")
    print(summary)

    # for cur_file in os.listdir(articles_dir):
    #     input_file_path = f"{articles_dir}/{cur_file}"
    #
    #     summarizer = SummarizeArticle(input_file_path)
    #     summary = summarizer.summarize()
    #     print("Summary of the file content:")
    #     print(summary.pretty_print())
    #     print("-----------")
    #     print(summary.content)
    #
    #     file_name = cur_file.split(".")[0]
    #     # Specify the file path
    #     file_path = f"../../../data/article_summaries/{file_name}.txt"
    #
    #     with open(file_path, "w") as file:
    #         file.write(summary.content)
