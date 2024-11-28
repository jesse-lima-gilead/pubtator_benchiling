import xml.etree.ElementTree as ET
from src.Prompts.PromptBuilder import PromptBuilder
from src.utils.logger import SingletonLogger
from src.llm_handler.llm_factory import LLMFactory
import os
import re
import json

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

    def get_clean_summary(self, response_content):
        try:
            # Parse the JSON string into a dictionary
            data = json.loads(response_content)
            # Extract the summary key
            clean_summary = data.get("summary", "No summary found")
        except json.JSONDecodeError as e:
            print("Error parsing JSON:", e)

        return clean_summary

    def summarize(self):
        summary_generated = False
        while not summary_generated:
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
            print(response_content)

            try:
                # Extract the JSON block using regex
                match = re.search(
                    r"```json(.*?)```", response_content, re.DOTALL | re.IGNORECASE
                )
                if match:
                    json_str = match.group(
                        1
                    ).strip()  # Extract the content inside ```json ... ```
                    # Validate and parse the JSON response
                    parsed_response = json.loads(json_str)
                    if (
                        isinstance(parsed_response, dict)
                        and "summary" in parsed_response
                    ):
                        clean_summary = parsed_response["summary"]
                        summary_generated = True
                        return clean_summary
                    else:
                        logger.error(
                            "Response JSON is not in the expected format. Trying again!"
                        )
                else:
                    logger.error(
                        "No valid JSON block found in the response. Trying again!"
                    )
            except json.JSONDecodeError:
                logger.error("Response contains invalid JSON. Trying again!")


# Usage
if __name__ == "__main__":
    input_file_path = "../../data/ner_processed/gnorm2_annotated/PMC_8418271.xml"
    summarizer = SummarizeArticle(input_file_path)
    summary = summarizer.summarize()
    print(f"\nArticle Summary:\n{summary}")

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
