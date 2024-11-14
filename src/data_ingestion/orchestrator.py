import os

from src.data_ingestion.pmc_articles_extractor import extract_pmc_articles
from src.data_ingestion.pmc_to_bioc_converter import convert_pmc_to_bioc
from src.data_ingestion.fetch_metadata import MetadataExtractor
from src.utils.logger import SingletonLogger

# from src.utils.s3_io_util import S3IOUtil

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
        # self.s3_io_util = S3IOUtil()

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
                    file_path=file_path,
                    metadata_path=metadata_path,
                    embeddings_model="pubmedbert",
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

        # # Save the PMC XML and BIOC XML to S3:
        # for pmc_file in os.listdir(self.pmc_local_path):
        #     if pmc_file.endswith(".xml"):
        #         self.s3_io_util.upload_file(
        #             file_path=os.path.join(self.pmc_local_path, pmc_file),
        #             object_name=f"pmc_full_text_articles/{pmc_file}",
        #         )
        #         logger.info(f"PMC XML file saved to S3: pmc_xml/{pmc_file}")
        #
        # for bioc_file in os.listdir(self.bioc_local_path):
        #     if bioc_file.endswith(".xml"):
        #         self.s3_io_util.upload_file(
        #             file_path=os.path.join(self.bioc_local_path, bioc_file),
        #             object_name=f"bioc_full_text_articles/{bioc_file}",
        #         )
        #         logger.info(f"BioC XML file saved to S3: bioc_xml/{bioc_file}")


# Example usage
if __name__ == "__main__":
    logger.info("Execution Started")
    query = "lung cancer"
    start_date = "2019"
    end_date = "2024"
    retmax = 25
    pmc_local_path: str = "../../test_data/pmc_full_text_articles"
    bioc_local_path: str = "../../test_data/bioc_full_text_articles"
    article_metadata_path = "../../test_data/pmc_full_text_metadata"
    article_ids = [
        "6468187",
        "7541005",
        "4154841",
        "6362936",
        "4904963",
        "4939985",
        "2538758",
        "5450131",
        "4176452",
        "7952801",
        "2528145",
        "7982715",
        "5093171",
        "6163012",
        "3251891",
        "4453850",
        "7327149",
        "8315452",
        "5736065",
        "5035793",
        "4488608",
        "6432898",
        "4273004",
        "7920504",
        "5087704",
        "4439943",
        "2361730",
        "3424666",
        "6521762",
        "7185216",
        "6956603",
        "4160987",
        "7937841",
        "3563999",
        "4286153",
        "6367552",
        "5231907",
        "5489751",
        "7536739",
        "4244293",
        "7366789",
        "7583132",
        "6519061",
        "6977168",
        "5355363",
        "4856797",
        "7955951",
        "7321680",
        "6463461",
        "3908495",
        "5482138",
        "5724586",
        "3756101",
        "7160534",
        "2361529",
        "3913798",
        "4869765",
        "6773970",
        "6642800",
        "6248272",
        "8137272",
        "6691185",
        "6358562",
        "4999419",
        "4580380",
        "3950870",
        "4317915",
        "4541544",
        "5835892",
        "5599204",
        "6924325",
        "3794963",
        "5384703",
        "4725461",
        "6202580",
        "6065473",
        "7371756",
        "6579872",
        "6947812",
        "7894017",
        "3940863",
        "7787955",
        "7046517",
        "7989961",
        "5483497",
        "3275680",
        "5320748",
        "3672380",
        "4632175",
        "6064590",
        "3469846",
        "8325052",
        "2694329",
        "7584802",
        "7807832",
        "7491043",
        "5304989",
        "4850750",
        "5772929",
        "7575440",
        "6303615",
        "5988498",
        "8111265",
        "5870096",
        "5561797",
        "5642389",
        "4856585",
        "6202542",
        "5125076",
        "2480972",
        "6658608",
        "7548235",
        "7102881",
        "5868625",
        "2905299",
        "7293078",
        "7734496",
        "5558684",
        "6781612",
        "5403387",
        "8334352",
        "3504940",
        "4606373",
        "4829131",
        "8132202",
        "7292488",
        "7671072",
        "3443515",
        "8057655",
        "3571730",
        "4970829",
        "7160558",
        "7932977",
        "7453501",
        "7540442",
        "8202655",
        "6064582",
        "4212561",
        "8312512",
    ]
    pmc_ingestor = PMCIngestor(
        query=query,
        article_ids=article_ids,
        start_date=start_date,
        end_date=end_date,
        pmc_local_path=pmc_local_path,
        bioc_local_path=bioc_local_path,
        article_metadata_path=article_metadata_path,
        retmax=retmax,
    )
    pmc_ingestor.run()
    logger.info("Execution Completed")
