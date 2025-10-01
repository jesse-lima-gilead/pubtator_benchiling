import random
import time
import xml.etree.ElementTree as ET
from typing import Optional

import requests
import torch
from transformers import pipeline

from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
import re
import html

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()
model_path_type = config_loader.get_config("paths")["model"]["type"]
model_path_config = config_loader.get_config("paths")["model"][model_path_type][
    "summarization_model"
]


def _clean_abstract_text(s: str) -> str:
    """
    Remove JATS/HTML tags, unescape entities, collapse whitespace.
    Keeps inner text of tags (e.g. <jats:italic>Word</jats:italic> -> Word).
    """
    if not s:
        return ""
    # remove XML/HTML tags but keep inner text
    # replace tags with a single space, which avoids accidentally joining words
    text = re.sub(r"<[^>]+>", " ", s)
    # unescape HTML entities
    text = html.unescape(text)
    # collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_doi_core(doi: str) -> str:
    """Return DOI core (10.x/...) from either '10.x/...' or 'https://doi.org/10.x/...'"""
    if not doi:
        return ""
    doi = doi.strip()
    if doi.lower().startswith("http"):
        parts = doi.split("/", 3)
        if len(parts) >= 4:
            return parts[3]
        return doi
    return doi


# ---------------------- Semantic Scholar fetcher ----------------------


def _fetch_semanticscholar_abstract(
    doi: str,
    timeout: int = 8,
    max_retries: int = 4,
    backoff_factor: float = 0.6,
) -> Optional[str]:
    """
    Try to fetch abstract from Semantic Scholar Graph API for a DOI.
    Returns raw abstract string (may contain markup) or None.
    Implements retries, backoff, jitter and honors Retry-After for 429 responses.
    """
    doi_core = _normalize_doi_core(doi)
    if not doi_core:
        return None

    url = (
        f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi_core}?fields=abstract"
    )
    ua = "metadata-fetcher/1.0"
    session = requests.Session()
    session.headers.update({"User-Agent": ua})

    def _sleep(attempt: int, retry_after: Optional[float] = None):
        if retry_after:
            logger.info(f"Semantic Scholar: sleeping Retry-After %s sec, {retry_after}")
            time.sleep(retry_after)
            return
        base = backoff_factor * (2**attempt)
        jitter = random.uniform(0, min(1.0, base * 0.5))
        sleep_time = base + jitter
        logger.info(f"Semantic Scholar: sleeping backoff, {sleep_time}, {attempt + 1}")
        time.sleep(sleep_time)

    for attempt in range(max_retries):
        try:
            logger.info(f"Semantic Scholar: GET, {url, attempt + 1}, {max_retries}")
            resp = session.get(url, timeout=timeout)
            status = resp.status_code
            if status == 200:
                jd = resp.json()
                abstract = jd.get("abstract")
                if abstract:
                    return abstract
                return None
            elif status == 429:
                # rate limited: honor Retry-After if provided
                ra = resp.headers.get("Retry-After")
                try:
                    ra_val = float(ra) if ra else None
                except Exception:
                    ra_val = None
                _sleep(attempt, retry_after=ra_val)
                continue
            elif 500 <= status < 600:
                _sleep(attempt)
                continue
            else:
                # other 4xx we treat as non-retryable
                logger.info(f"Semantic Scholar: returned status; giving up, {status}")
                return None
        except requests.RequestException as e:
            logger.info(f"Semantic Scholar request exception: %r, {e}")
            _sleep(attempt)
            continue
    logger.info("Semantic Scholar: all attempts exhausted")
    return None


class SummarizeArticle:
    """Class to load Pubmed Article's content and Summarize it."""

    def __init__(
        self,
        input_file_path: str,
        file_handler: FileHandler,
        doi: str,
        metadata_abstract: str,
        model_name: str = "mistral_7b",
        summarization_pipe=None,
    ):
        self.input_file_path = input_file_path
        self.file_handler = file_handler
        self.abstract_text = self._get_abstract(doi, metadata_abstract)
        self.prompt_template = (
            "Given the abstract of an article, write a concise summary of the following in about 60 words:\n"
            f"{self.abstract_text}\n"
            "Enclose the summary exactly in <<< and >>>, with no other text."
        )
        self.max_retries = 3

        if summarization_pipe:
            self.pipe = summarization_pipe
        else:
            logger.warn("Loading summarization model at runtime...")
            model_path = self._get_model_info(model_name=model_name)
            # Use GPU if available, else CPU
            device = 0 if torch.cuda.is_available() else -1
            logger.info(f"Using device: {device}")
            self.pipe = pipeline(
                "text-generation",
                model=model_path,
                device_map="auto",
                max_new_tokens=1000,
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

    def _load_file_content(self, abstract_only=True) -> str:
        """Parses the XML file and extracts relevant text content."""
        try:
            tree = self.file_handler.parse_xml_file(self.input_file_path)
            root = tree.getroot()

            content = ""
            for passage in root.findall(".//passage"):
                infon = passage.find("infon")
                if abstract_only:
                    # check if key attribute starts with "abstract"
                    if (
                        infon is not None
                        and infon.text
                        and infon.text.startswith("abstract")
                    ):
                        return passage.findtext("text").strip()
                else:
                    passage_text = passage.findtext("text")
                    if passage_text:
                        content += passage_text + "\n"
            return content.strip()

        except ET.ParseError as e:
            raise ValueError(f"Failed to parse XML: {e}")
        except FileNotFoundError:
            raise ValueError(f"File not found: {self.input_file_path}")

    def _get_abstract(self, doi, metadata_abstract):
        abstract = _fetch_semanticscholar_abstract(doi)
        if abstract is None or abstract == "":
            abstract = metadata_abstract

        if abstract is None or abstract == "":
            abstract = self._load_file_content()

        abstract = _clean_abstract_text(abstract)

        return abstract

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
        # [0]["generated_text"] -> [{'role': 'user',
        #   'content': 'Given the abstract of an article, write a concise summary of the following in about 60 words:\nActivation of kinases signalling pathways contributes to various malignant phenotypes in human cancers, including breast tumour. To examine the possible activation of these signalling molecules, we examined the phosphorylation status in 12 protein kinases and transcription factors in normal primary human mammary epithelial cells, telomerase-immortalised human breast epithelial cell line, and two breast cancer lines, MDA-MB-468 and MCF-7, using Kinexus phosphorylated protein screening assays. The phosphorylation of FAK, mTOR, p70S6K, and PDK-1 were elevated in both breast cancer cell lines, whereas the phosphorylation of AKT, EGFR, ErbB2/Her2, PDGFR, Shc, and Stat3 were elevated in only one breast cancer line compared to normal primary mammary epithelial cells and telomerase-immortalised breast epithelial cells. The same findings were confirmed by Western blotting and by kinase assays. We further substantiated the phosphorylation status of these molecules in tissue microarray slides containing 89 invasive breast cancer tissues as well as six normal mammary tissues with immunohistochemistry staining using phospho-specific antibodies. Consistent findings were obtained as greater than 70% of invasive breast carcinomas expressed moderate to high levels of phosphorylated PDK-1, AKT, p70S6K, and EGFR. In sharp contrast, phosphorylation of the same proteins was nearly undetectable or was at low levels in normal mammary tissues under the same assay. Elevated phosphorylation of PDK-1, AKT, mTOR, p70S6K, S6, EGFR, and Stat3 were highly associated with invasive breast tumours (P&lt;0.05). Taken together, our results suggest that activation of these kinase pathways by phosphorylation may in part account for molecular pathogenesis of human breast carcinoma. Particularly, moderate to high level of PDK-1 phosphorylation was found in 86% of high-grade metastasised breast tumours. This is the first report demonstrating phosphorylation of PDK-1 is frequently elevated in breast cancer with concomitantly increased phosphorylation of downstream kinases, including AKT, mTOR, p70S6K, S6, and Stat3. This finding thus suggested PDK-1 may promote oncogenesis in part through the activation of AKT and p70S6K and rationalised that PDK-1 as well as downstream components of PDK-1 signalling pathway may be promising therapeutic targets to treat breast cancer.\nEnclose the summary exactly in <<< and >>>, with no other text.'},
        #  {'role': 'assistant',
        #   'content': ' <<< The study reveals elevated phosphorylation of several kinases and transcription factors in'}]
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


def preprint_articles_summarizer(
    preprint_file_path: str,
    summary_path: str,
    metadata_infons: dict,
    file_handler: FileHandler,
    write_to_s3: bool,
    s3_summary_path: str,
    s3_file_handler: FileHandler,
    summarization_pipe=None,
):
    doi = metadata_infons.get("doi")
    metadata_abstract = metadata_infons.get("abstract")
    summarizer = SummarizeArticle(
        input_file_path=preprint_file_path,
        file_handler=file_handler,
        doi=doi,
        metadata_abstract=metadata_abstract,
        summarization_pipe=summarization_pipe,
    )
    summary = summarizer.summarize()
    # for testing
    # summary = summarizer.abstract_text
    file = preprint_file_path.split("/")[-1]
    summary_file_name = file.replace(".pdf", ".txt")
    summary_file_path = file_handler.get_file_path(summary_path, summary_file_name)
    file_handler.write_file(summary_file_path, summary)
    logger.info(f"Summary generated for: {file}")

    if write_to_s3:
        # Save to S3
        s3_summary_file_path = s3_file_handler.get_file_path(
            s3_summary_path, summary_file_name
        )
        s3_file_handler.write_file(s3_summary_file_path, summary)
        logger.info(f"Summary saved to S3: {s3_summary_file_path}")


# # Usage
# if __name__ == "__main__":
#     # Initialize the config loader
#     config_loader = YAMLConfigLoader()
#
#     # Retrieve paths config
#     paths_config = config_loader.get_config("paths")
#     storage_type = paths_config["storage"]["type"]
#
#     # Get file handler instance from factory
#     file_handler = FileHandlerFactory.get_handler(storage_type)
#     # Retrieve paths from config
#     paths = paths_config["storage"][storage_type]
#
#     article_ids = ["PMC_2538758", "PMC_7614604", "PMC_4439943"]
#
#     from pathlib import Path
#
#     # Directories
#     input_dir = Path("../../../old_data/staging/bioc_xml")
#     output_dir = Path("../../../old_data/articles_metadata/summary")
#
#     # Process each XML file in input_dir
#     for xml_file in input_dir.glob("*.xml"):
#         if xml_file.stem not in article_ids:
#             continue
#         # Compute corresponding output file path
#         summary_filename = xml_file.stem + ".txt"
#         output_file_path = output_dir / summary_filename
#
#         try:
#             print(f"Processing {xml_file.name}...")
#             summarizer = SummarizeArticle(str(xml_file), file_handler)
#             summary = summarizer.summarize()
#
#             # Write summary to output file
#             with open(output_file_path, "w", encoding="utf-8") as f:
#                 f.write(summary)
#
#             print(f" Saved summary to {output_file_path}")
#
#         except Exception as e:
#             print(f" Failed to process {xml_file.name}: {e}")
#
#     print("Batch summarization completed.")
#
#     # summarizer = SummarizeArticle(input_file_path, file_handler)
#     # summary = summarizer.summarize()
#     # print(f"\nArticle Summary:\n{summary}")
#     # with open(output_file_path, "w") as file:
#     #     file.write(summary)
#
#     # for cur_file in os.listdir(articles_dir):
#     #     input_file_path = f"{articles_dir}/{cur_file}"
#     #
#     #     summarizer = SummarizeArticle(input_file_path)
#     #     summary = summarizer.summarize()
#     #     print("Summary of the file content:")
#     #     print(summary.pretty_print())
#     #     print("-----------")
#     #     print(summary.content)
#     #
#     #     file_name = cur_file.split(".")[0]
#     #     # Specify the file path
#     #     file_path = f"../../../data/article_summaries/{file_name}.txt"
#     #
#     #     with open(file_path, "w") as file:
#     #         file.write(summary.content)
