import xml.etree.ElementTree as ET
from src.pubtator_utils.prompts_handler.PromptBuilder import PromptBuilder
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.llm_handler.llm_factory import LLMFactory
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
import re
import json

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class SummarizeArticle:
    """Class to load Pubmed Article's content and Summarize it."""

    def __init__(self, input_file_path: str, file_handler: FileHandler):
        self.prompt_builder = PromptBuilder()
        self.input_file_path = input_file_path
        self.file_handler = file_handler
        self.pmc_article_text = self._load_file_content()
        llm_factory = LLMFactory()
        llm_handler = llm_factory.create_llm(llm_type="BedrockClaude")
        self.query_llm = llm_handler.get_query_llm()

    def _load_file_content(self) -> str:
        """Parses the XML file and extracts relevant text content."""
        try:
            tree = self.file_handler.parse_xml_file(self.input_file_path)
            # tree = ET.parse(self.input_file_path)
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
        return response_content


# Usage
if __name__ == "__main__":
    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]
    input_file_path = "../../data/thal_extra_staging/bioc_xml/PMC_128942.xml"
    output_file_path = "../../data/thal_extra_articles_metadata/summary/PMC_128942.txt"
    summarizer = SummarizeArticle(input_file_path)
    summary = summarizer.summarize()
    print(f"\nArticle Summary:\n{summary}")
    with open(output_file_path, "w") as file:
        file.write(summary)

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
