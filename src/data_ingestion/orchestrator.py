import os

from src.data_ingestion.pmc_articles_extractor import extract_pmc_articles
from src.data_ingestion.pmc_to_bioc_converter import convert_pmc_to_bioc
from src.data_ingestion.fetch_metadata import MetadataExtractor
from src.utils.logger import SingletonLogger
from src.utils.s3_io_util import S3IOUtil

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PMCIngestor:
    def __init__(
        self,
        query: str,
        start_date: str,
        end_date: str,
        retmax: int = 50,
        article_ids: list = [],
        pmc_local_path: str = "../../data/pmc_full_text_articles",
        bioc_local_path: str = "../../data/bioc_xml",
        article_metadata_path: str = "../../data/article_metadata",
    ):
        self.query = query
        self.article_ids = article_ids
        self.start_date = start_date
        self.end_date = end_date
        self.retmax = retmax
        self.pmc_local_path = pmc_local_path
        self.bioc_local_path = bioc_local_path
        self.article_metadata_path = article_metadata_path
        self.s3_io_util = S3IOUtil()

    # Runs the combined process
    def run(self):
        # Extract the free full text articles from PMC:
        extract_pmc_articles(
            query=self.query,
            article_ids=self.article_ids,
            start_date=self.start_date,
            end_date=self.end_date,
            pmc_local_path=self.pmc_local_path,
            retmax=self.retmax,
        )

        # Fetch and store metadata of extracted articles
        for file in os.listdir(self.pmc_local_path):
            if file.endswith(".xml"):
                file_path = os.path.join(self.pmc_local_path, file)
                metadata_path = os.path.join(
                    self.article_metadata_path, file.replace(".xml", "_metadata.json")
                )
                metadata_extractor = MetadataExtractor(
                    file_path=file_path, metadata_path=metadata_path
                )
                # metadata = metadata_extractor.parse_xml()
                # metadata_extractor.save_metadata_as_json()
                # logger.info(f"Metadata saved as JSON: {metadata}")
                metadata_extractor.save_metadata_to_vector_db()
                logger.info(f"Metadata for {file} saved to Vector DB")

        # Convert the PMC Articles to BioC File Format:
        for file in os.listdir(self.pmc_local_path):
            if file.endswith(".xml"):
                convert_pmc_to_bioc(
                    os.path.join(self.pmc_local_path, file), self.bioc_local_path
                )

        # Save the PMC XML and BIOC XML to S3:
        for pmc_file in os.listdir(self.pmc_local_path):
            if pmc_file.endswith(".xml"):
                self.s3_io_util.upload_file(
                    file_path=os.path.join(self.pmc_local_path, pmc_file),
                    object_name=f"pmc_full_text_articles/{pmc_file}",
                )
                logger.info(f"PMC XML file saved to S3: pmc_xml/{pmc_file}")

        for bioc_file in os.listdir(self.bioc_local_path):
            if bioc_file.endswith(".xml"):
                self.s3_io_util.upload_file(
                    file_path=os.path.join(self.bioc_local_path, bioc_file),
                    object_name=f"bioc_full_text_articles/{bioc_file}",
                )
                logger.info(f"BioC XML file saved to S3: bioc_xml/{bioc_file}")


# Example usage
if __name__ == "__main__":
    logger.info("Execution Started")
    query = "lung cancer"
    start_date = "2019"
    end_date = "2024"
    retmax = 25
    pmc_local_path: str = "../../test_data/pmc_full_text_articles"
    bioc_local_path: str = "../../test_data/bioc_full_text_articles"
    article_ids = []
    pmc_ingestor = PMCIngestor(
        query=query,
        article_ids=article_ids,
        start_date=start_date,
        end_date=end_date,
        pmc_local_path=pmc_local_path,
        bioc_local_path=bioc_local_path,
        retmax=retmax,
    )
    pmc_ingestor.run()
    logger.info("Execution Completed")
