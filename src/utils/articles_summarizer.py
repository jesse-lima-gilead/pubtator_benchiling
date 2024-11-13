import xml.etree.ElementTree as ET
from src.Prompts.PromptBuilder import PromptBuilder
from src.utils.logger import SingletonLogger

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class SummarizeArticle:
    """Class to load Pubmed Article's content and Summarize it."""

    def __init__(self, input_file_path: str):
        self.prompt_builder = PromptBuilder()
        self.input_file_path = input_file_path
        self.pmc_article_text = self._load_file_content()

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

        logger.info(f"Generated response: {llm_summary_response}")
        return llm_summary_response


# # Usage
# if __name__ == "__main__":
#     articles_dir = "../../../data/gnorm2_annotated/bioformer_annotated"
#     for cur_file in os.listdir(articles_dir):
#         input_file_path = f"{articles_dir}/{cur_file}"
#
#         summarizer = SummarizeArticle(input_file_path)
#         summary = summarizer.summarize()
#         print("Summary of the file content:")
#         print(summary.pretty_print())
#         print("-----------")
#         print(summary.content)
#
#         file_name = cur_file.split(".")[0]
#         # Specify the file path
#         file_path = f"../../../data/article_summaries/{file_name}.txt"
#
#         with open(file_path, "w") as file:
#             file.write(summary.content)
