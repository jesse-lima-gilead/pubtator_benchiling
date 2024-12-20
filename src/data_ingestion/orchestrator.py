import os

from src.data_ingestion.pmc_articles_extractor import extract_pmc_articles
from src.data_ingestion.pmc_to_bioc_converter import convert_pmc_to_bioc
from src.data_ingestion.fetch_metadata import MetadataExtractor
from src.data_ingestion.articles_summarizer import SummarizeArticle
from src.data_ingestion.prettify_xml import XMLFormatter
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
        pmc_local_path: str,
        bioc_local_path: str,
        article_metadata_path: str,
        retmax: int = 50,
        article_ids: list = [],
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
        logger.info("Extracting PMC Articles...")
        extract_pmc_articles(
            query=self.query,
            article_ids=self.article_ids,
            start_date=self.start_date,
            end_date=self.end_date,
            pmc_local_path=self.pmc_local_path,
            retmax=self.retmax,
        )

        # Fetch and store metadata of extracted articles
        logger.info("Fetching metadata for the articles...")
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
        logger.info("Converting PMC Articles to BioC XML...")
        for file in os.listdir(self.pmc_local_path):
            if file.endswith(".xml"):
                convert_pmc_to_bioc(
                    os.path.join(self.pmc_local_path, file), self.bioc_local_path
                )

        # Prettify the BioC XML files:
        # logger.info("Prettifying the BioC XML files...")
        # formatter = XMLFormatter(folder_path=self.bioc_local_path)
        # formatter.process_folder()

        # Generate articles summaries
        logger.info("Generating summaries for the articles using BioC XMLs...")
        for file in os.listdir(self.bioc_local_path):
            if file.endswith(".xml"):
                logger.info(f"Generating summary for: {file}")
                file_path = os.path.join(self.bioc_local_path, file)
                summarizer = SummarizeArticle(input_file_path=file_path)
                summary = summarizer.summarize()
                summary_file_path = os.path.join(
                    self.article_metadata_path,
                    f"summary/{file.replace('.xml', '.txt')}",
                )
                with open(summary_file_path, "w") as f:
                    f.write(summary)

                logger.info(f"Summary generated for: {file}")

        # Save the PMC XML, BIOC XML, Metadata (Summary) to S3:
        for pmc_file in os.listdir(self.pmc_local_path):
            if pmc_file.endswith(".xml"):
                self.s3_io_util.upload_file(
                    file_path=os.path.join(self.pmc_local_path, pmc_file),
                    object_name=f"archive/pmc_full_text_articles/{pmc_file}",
                )
                logger.info(f"PMC XML file saved to S3: archive/pmc_full_text_articles/{pmc_file}")

        for bioc_file in os.listdir(self.bioc_local_path):
            if bioc_file.endswith(".xml"):
                self.s3_io_util.upload_file(
                    file_path=os.path.join(self.bioc_local_path, bioc_file),
                    object_name=f"bioc_full_text_articles/{bioc_file}",
                )
                logger.info(f"BioC XML file saved to S3: bioc_full_text_articles/{bioc_file}")

        summary_file_path = f"{self.article_metadata_path}/summary"
        for summary_file in os.listdir(summary_file_path):
            if summary_file.endswith(".txt"):
                self.s3_io_util.upload_file(
                    file_path=os.path.join(summary_file_path, summary_file),
                    object_name=f"summary/{summary_file}",
                )
                logger.info(f"Summary file saved to S3: summary/{summary_file}")


# Example usage
if __name__ == "__main__":
    logger.info("Execution Started")
    query = ""
    start_date = "2019"
    end_date = "2024"
    retmax = 25
    pmc_local_path: str = "../../data/poc_dataset/staging/pmc_xml"
    bioc_local_path: str = "../../data/poc_dataset/staging/bioc_xml"
    article_metadata_path = "../../data/poc_dataset/articles_metadata"

    golden_dataset_article_ids = ['2361529', '8325052', '5093171', '3251891', '5450131', '6519061', '7160534', '6947812', '4154841', '4317915', '5642389', '3504940', '5384703', '7671072', '8057655', '2538758', '7932977', '4856585', '5403387', '7327149', '7575440', '6642800', '5724586', '5087704', '6202580', '5482138', '5870096', '4869765', '5988498', '4856797', '4453850', '6362936', '3424666', '6202542', '5868625', '6248272', '3756101', '5355363', '4970829', '6691185', '7584802', '4850750', '6358562', '7371756', '3940863', '6367552', '4999419', '5231907', '4212561', '6579872', '8111265', '6432898', '7807832', '5489751', '4160987', '5125076', '4829131', '3469846', '6521762', '6065473', '4725461', '6064590', '6658608', '5558684', '6924325', '8312512', '8137272', '6163012', '7937841', '4244293', '8315452', '6303615', '7102881', '4273004', '5483497', '3443515', '6977168', '6956603', '6064582', '7366789', '5320748', '5561797', '4541544', '7787955', '5736065', '5304989', '4176452', '7920504', '3563999', '3913798', '7955951', '6781612', '5599204', '7536739', '7952801', '7540442', '6468187', '4439943', '8202655', '7293078', '7160558', '7548235', '3672380', '7541005', '2694329', '4488608', '7989961', '4606373', '2528145', '4632175', '7982715', '7894017', '3275680', '7491043', '7185216', '3908495', '4904963', '2905299', '5772929', '7583132', '4939985', '7453501', '7321680', '5835892', '8334352', '4286153', '6773970', '7046517', '6463461', '2480972', '7734496', '3571730', '5035793', '8132202', '4580380']

    enhanced_golden_dataset_article_ids = ['10923916', '10915134', '8748704', '10831337', '9413286', '10698546', '11245995', '11116757', '9896310', '10870877', '10645594', '10017705', '10897627', '10370087', '8418271', '11231252', '11118283', '10907391', '10232659', '10912034', '10443631', '11371094', '9161072', '11014662', '8579308', '10619435', '9746914', '10117631', '9050543', '11008188', '11073880', '11387199', '9674284', '11405273', '11208295', '11319832', '10837166']

    litqa_dataset_articles_id = ['10917522', '10871260', '10024153', '10897447', '10530073', '10773119', '11078116', '11009116', '10888836', '10764289', '8995031', '10990113', '10830414', '6919571', '10539709', '9350683', '11077751', '9281392', '10954967', '8809391', '10359266', '11067025', '11111400', '11100620', '11062927', '9945969', '11009413', '11078759', '10264397', '11077091', '11113265', '11077085', '11237425', '11077093', '9452294', '11062931', '9746304', '10322706', '11101474', '11077092', '10636190', '10618286', '11107001', '10760936', '9563306', '10987551', '10869271', '9918738', '10942590', '10765522', '11014256', '9797404', '9949159', '11066985', '10491752', '10409721', '7649238', '11101664', '10862896', '11041701', '10393267', '11326130', '10855626', '11062907', '11077059', '11077071', '10983909', '11098102', '10944506', '11194073', '10912769', '11077075', '10131484', '11077077', '10789797', '9729108', '11188114', '10870475', '10927504', '10083194', '10473641', '10916875', '7614541', '11573686', '11021478', '10988632', '9605863', '8584034', '9130329', '9438555', '10730565', '10686827', '11111397', '10261898', '9021019', '9970000', '10719092', '11136670', '10439884', '10495415', '10312723', '10748745', '9833661', '10387117', '9683785']

    poc_dataset_articles_id = ['7614604', '10163802', '9098652', '10452697', '10213952']

    pmc_ingestor = PMCIngestor(
        query=query,
        article_ids=poc_dataset_articles_id,
        start_date=start_date,
        end_date=end_date,
        pmc_local_path=pmc_local_path,
        bioc_local_path=bioc_local_path,
        article_metadata_path=article_metadata_path,
        retmax=retmax,
    )
    pmc_ingestor.run()
    logger.info("Execution Completed")
