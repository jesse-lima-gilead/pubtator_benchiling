import os
from bs4 import BeautifulSoup, Tag, NavigableString
import bioc
from bioc import BioCCollection, BioCDocument, BioCPassage
import json
import re
from datetime import date
import html
import re
import unicodedata
import json
from typing import Optional
import bioc
from typing import List, Tuple, Optional

from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

from pathlib import Path

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve paths config
paths = config_loader.get_config("paths")
storage_type = paths["storage"]["type"]

# Get file handler instance from factory
file_handler = FileHandlerFactory.get_handler(storage_type)
# Retrieve paths from config
paths_config = paths["storage"][storage_type]

# ------------------ Converter Helpers -----------------------


def convert_eln_html_to_bioc(
    eln_interim_path: str,
    bioc_path: str,
):
    converted_articles_count = 0
    for eln_html_dir in os.listdir(eln_interim_path):
        eln_html_dir_path = Path(eln_interim_path) / eln_html_dir
        eln_html_file_path = eln_html_dir_path / (eln_html_dir + ".html")
        eln_html_file_name = eln_html_dir + ".html"
        eln_bioc_xml_file_path = Path(bioc_path) / (eln_html_dir + ".xml")
        if os.path.exists(eln_html_file_path):
            pass
        else:
            logger.warning(
                f"HTML file not found: {eln_html_file_name}, skipping conversion."
            )
    return converted_articles_count
