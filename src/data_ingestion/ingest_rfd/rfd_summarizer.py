# Read the BioC File from bioC path and extract the passage whose section title matches the regex 'executive summary'
# or 'summary' or 'abstract'

import os
import xml.etree.ElementTree as ET
import torch
from transformers import pipeline
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
import re

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()
model_path_config = config_loader.get_config("paths")["model"]["summarization_model"]


class SummarizeArticle:
    """Class to load RFD Article's content and Summarize it."""

    def __init__(
        self,
        input_file_path: str,
        file_handler: FileHandler,
        model_name: str = "mistral_7b",
    ):
        self.input_file_path = input_file_path
        self.file_handler = file_handler
        self.abstract_text = self._load_file_content()
        self.prompt_template = (
            "Given the abstract of an article, write a concise summary of the following in about 60-80 words:\n"
            f"{self.abstract_text}\n"
            "Enclose the summary exactly in <<< and >>>, with no other text."
        )
        self.max_retries = 3

        # Comment for testing:
        model_path = self._get_model_info(model_name=model_name)

        # Use GPU if available, else CPU
        device = 0 if torch.cuda.is_available() else -1
        logger.info(f"Using device: {device}")
        # self.pipe = pipeline("text-generation", model=model_path, device=device, max_new_tokens=1000)
        self.pipe = pipeline(
            "text-generation", model=model_path, device_map="auto", max_new_tokens=1000
        )

    def _extract_summary(self, text):
        match = re.search(r"<<<(.*?)>>>", text, re.DOTALL)
        return match.group(1).strip() if match else None

    def _count_words(self, text):
        return len(text.split())

    def _get_model_info(self, model_name: str):
        try:
            model_path = model_path_config[model_name]["model_path"]
            logger.info(f"Model Loaded from {model_path}")
            return model_path
        except Exception as e:
            raise ValueError(f"Error loading model {model_name}: {e}")

    def _load_file_content(self) -> str:
        """Parses the XML file and extracts relevant text content."""
        try:
            tree = self.file_handler.parse_xml_file(self.input_file_path)
            root = tree.getroot()

            content = ""
            # extract the passage whose section title matches the regex 'executive summary' or 'summary' or 'abstract'
            for passage in root.findall(".//passage"):
                section_title = passage.find("./infon[@key='section_title']")
                if section_title is not None:
                    title_text = section_title.text.lower()
                    if re.search(r"\b(executive summary)\b", title_text, re.IGNORECASE):
                        text_elem = passage.find("text")
                        if text_elem is not None and text_elem.text:
                            content += text_elem.text + " "
            return content.strip()

        except ET.ParseError as e:
            raise ValueError(f"Failed to parse XML: {e}")
        except FileNotFoundError:
            raise ValueError(f"File not found: {self.input_file_path}")

    def _fallback_summary(self, max_words=80):
        lines = self.abstract_text.strip().split(". ")  # Split on sentences
        selected_lines = []
        word_count = 0

        for line in lines:
            line_word_count = len(line.split())
            if word_count + line_word_count > max_words:
                break
            selected_lines.append(line)
            word_count += line_word_count

        # Ensure at least 2 lines are included
        if len(selected_lines) < 2 and len(lines) >= 2:
            selected_lines = lines[:2]

        abs_summary = ". ".join(selected_lines).strip()
        logger.info(
            f"Generate a {len(abs_summary.split())} Words Summary from the Abstract"
        )

        return abs_summary

    def summarize(self):
        messages = [{"role": "user", "content": self.prompt_template}]
        is_summary_generated = False

        for attempt in range(self.max_retries):
            logger.info(f"Attempt {attempt + 1}...")

            try:
                if attempt == 0:
                    response = self.pipe(messages)[0]["generated_text"][-1]["content"]
                elif is_summary_generated:
                    # Add last assistant response and ask for significantly shorter version
                    messages.append(
                        {"role": "assistant", "content": last_summary_wrapped}
                    )
                    messages.append(
                        {
                            "role": "user",
                            "content": "Shorten it further so that it's significantly briefer.",
                        }
                    )
                    response = self.pipe(messages)[0]["generated_text"][-1]["content"]
                else:  # this time with more strict prompt
                    response = self.pipe(messages)[0]["generated_text"][-1]["content"]

                # Extract summary
                summary = self._extract_summary(response)

                if summary:
                    is_summary_generated = True
                    word_count = self._count_words(summary)
                    logger.info(f"Summary: {summary}")
                    logger.info(f"Word count: {word_count}")

                    if word_count <= 80:
                        return summary
                    else:
                        last_summary_wrapped = f"<<<{summary}>>>"
                else:
                    # Retry with stricter rephrasing if <<< >>> not found
                    messages = [
                        {
                            "role": "user",
                            "content": self.prompt_template
                            + "\nPlease strictly include the summary within <<< and >>>.",
                        }
                    ]
            except Exception as e:
                logger.error(f"Error during summarization attempt {attempt + 1}: {e}")

        logger.info("Failed to generate a valid summary. Using fallback from abstract.")
        return self._fallback_summary()


def summarizer_rfd(
    rfd_file_name: str,
    bioc_path: str,
    summary_path: str,
    file_handler: FileHandler,
):
    try:
        logger.info(f"Summarizing {rfd_file_name}...")
        article_summarizer = SummarizeArticle(
            input_file_path=bioc_path,
            file_handler=file_handler,
        )

        # For Testing:
        rfd_summary = article_summarizer.abstract_text

        # Actual
        # rfd_summary = article_summarizer.summarize()

        if rfd_summary:
            summary_file_name = rfd_file_name.split(".")[0] + ".txt"
            summary_file_path = os.path.join(summary_path, summary_file_name)
            file_handler.write_file(summary_file_path, rfd_summary)
            logger.info(f"RFD Summary saved to {summary_file_path}")
            return True
        else:
            logger.warning(f"No summary generated for RFD {rfd_file_name}")
            return False

    except Exception as e:
        logger.error(f"Error in rfd_summarizer: {e}")
        raise


def rfd_summarizer(bioc_path: str, summary_path: str, file_handler: FileHandler):
    try:
        summary_cnt = 0
        for rfd_bioc_xml in os.listdir(bioc_path):
            if rfd_bioc_xml.endswith(".xml"):
                logger.info(f"Reading {rfd_bioc_xml}...")
                rfd_file_name = rfd_bioc_xml.replace(".xml", "")
                bioc_file_path = os.path.join(bioc_path, rfd_bioc_xml)
                is_summary_generated = summarizer_rfd(
                    rfd_file_name=rfd_file_name,
                    bioc_path=bioc_file_path,
                    summary_path=summary_path,
                    file_handler=file_handler,
                )
                if is_summary_generated:
                    summary_cnt += 1
        return summary_cnt
    except Exception as e:
        logger.error(f"Error in rfd_summarizer: {e}")
        raise


if __name__ == "__main__":
    # Example usage
    local_file_handler = FileHandlerFactory.get_file_handler("local")
    s3_file_handler = FileHandlerFactory.get_file_handler("s3")

    summarizer_rfd(
        bioc_path="path/to/local/bioc_file.xml",
        summary_path="path/to/local/rfd_summary.txt",
        file_handler=local_file_handler,
        write_to_s3=True,
        s3_summary_path="path/to/s3/rfd_summary.txt",
        s3_file_handler=s3_file_handler,
    )
