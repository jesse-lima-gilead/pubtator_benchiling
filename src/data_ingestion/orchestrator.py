import os

from src.data_ingestion.pmc_articles_extractor import extract_pmc_articles
from src.data_ingestion.pmc_to_bioc_converter import convert_pmc_to_bioc
from src.data_ingestion.fetch_metadata import MetadataExtractor
from src.data_ingestion.articles_summarizer import SummarizeArticle
from src.data_ingestion.prettify_xml import XMLFormatter
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

        # # Save the PMC XML, BIOC XML, Metadata (Summary) to S3:
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
    query = ""
    start_date = "2019"
    end_date = "2024"
    retmax = 25
    pmc_local_path: str = "../../litqa_data/staging/pmc_xml"
    bioc_local_path: str = "../../litqa_data/staging/bioc_xml"
    article_metadata_path = "../../litqa_data/articles_metadata"
    golden_dataset_article_ids = [
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

    extra_articles_id = [
        "9413286",
        "7444693",
        "7442721",
        "7737765",
        "9050543",
        "8748704",
        "9167747",
        "9161072",
        "9746914",
        "9674284",
        "9896310",
        "11332722",
        "10117631",
        "10017705",
        "10232659",
        "10443631",
        "10698546",
        "10870877",
        "10645594",
        "10831337",
        "10912034",
        "10907391",
        "10837166",
        "10897627",
        "10915134",
        "10923916",
        "11208295",
        "11319832",
        "11008188",
        "11014662",
        "11116757",
        "11118283",
        "11231252",
        "11245995",
        "11371094",
        "11405273",
        "11387199",
        "8418271",
        "8579308",
        "8784611",
        "10119142",
        "10370087",
        "10619435",
        "11073880",
        "8698540",
    ]

    litqa_dataset_articles_id = [
        "11100620",
        "9729108",
        "3479395",
        "10954967",
        "9350683",
        "9034188",
        "11111400",
        "11573686",
        "11077091",
        "11573686",
        "11062907",
        "11092668",
        "10916875",
        "9469465",
        "8779329",
        "11062927",
        "11098688",
        "11098102",
        "10888836",
        "10748745",
        "9918738",
        "11113265",
        #'8809391',
        "9452294",
        "10530073",
        "9812260",
        "11066985",
        "11009116",
        "11077059",
        "7614541",
        "11077077",
        "11041766",
        "10680751",
        "9109419",
        "9833661",
        "11573686",
        "11136670",
        "10359266",
        "11573686",
        "11014256",
        "9012689",
        "10830414",
        "10764289",
        "8944923",
        "11077093",
        "9797404",
        "11062931",
        "10987551",
        "9563306",
        "11573686",
        "10855626",
        "10225359",
        "7649238",
        "9970000",
        "6927209",
        "11077751",
        "10491752",
        "10720985",
        "9438555",
        "10409721",
        "11021478",
        "10871290",
        "10719092",
        "8988390",
        "8454890",
        "10897447",
        "10990113",
        "11118371",
        #'6919571',
        "11077071",
        "10312723",
        "10393267",
        "9017300",
        "10083194",
        "10942590",
        "9262863",
        "10869271",
        #'9281392',
        "10773119",
        "11077092",
        #'8995031',
        "10927504",
        "11078759",
        "11573686",
        "10663388",
        "9021019",
        "11067025",
        "10322706",
        "10870475",
        "11041701",
        "10765522",
        "11573686",
        "11237425",
        "9945969",
        "8813897",
        "10618286",
        "8815333",
        "10871260",
        "8633126",
        "11326130",
        "11188114",
        "9949159",
        "10912769",
        "10501127",
        "11416571",
        "10983909",
        "10091861",
        "11573686",
        "11573686",
        "10387117",
        "10917522",
        "10024153",
        "10439884",
        "11079864",
        "10862838",
        "10862896",
        "9130329",
        "9683785",
        "11077075",
        "11573686",
        "10760936",
        "10495415",
        "10789797",
        "10636190",
        "11573686",
        "10261898",
        "10344468",
        "10473641",
        "9357393",
        "11101664",
        "10730565",
        "11118428",
        "11107001",
        "11078116",
        "10484233",
        "7986245",
        "11101474",
        "11573686",
        "11573686",
        "11573686",
        "8584034",
        "10264397",
        "11009413",
        "11573686",
        "10131484",
        "11573686",
        "11077085",
        "11194073",
        "11573686",
        "10988632",
        "10944506",
        "9605863",
        "11111397",
        "11187695",
        "7611557",
        "10686827",
        "11349933",
        "8842852",
        "9746304",
        "478567",
        "11573686",
        "10539709",
    ]

    pmc_ingestor = PMCIngestor(
        query=query,
        article_ids=litqa_dataset_articles_id,
        start_date=start_date,
        end_date=end_date,
        pmc_local_path=pmc_local_path,
        bioc_local_path=bioc_local_path,
        article_metadata_path=article_metadata_path,
        retmax=retmax,
    )
    pmc_ingestor.run()
    logger.info("Execution Completed")
