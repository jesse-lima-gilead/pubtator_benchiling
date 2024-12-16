import csv
import os.path
from collections import defaultdict
from typing import List, Dict
import json
from src.vector_db_handler.qdrant_handler import QdrantHandler
from src.Prompts.PromptBuilder import PromptBuilder
from src.data_processing.embedding.embeddings_handler import (
    get_embeddings,
    get_model_info,
)
from src.utils.config_reader import YAMLConfigLoader
from src.utils.logger import SingletonLogger
from src.llm_handler.llm_factory import LLMFactory

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
vectordb_config = config_loader.get_config("vectordb")["qdrant"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def initialize_article_qdrant_manager(embeddings_collection_type: str):
    # Initialize the QdrantHandler
    qdrant_handler = QdrantHandler(
        collection_type=embeddings_collection_type, params=vectordb_config
    )
    qdrant_manager = qdrant_handler.get_qdrant_manager()
    return qdrant_manager


def initialize_metadata_qdrant_manager(metadata_collection_type: str):
    # Initialize the QdrantHandler
    qdrant_handler = QdrantHandler(
        collection_type=metadata_collection_type, params=vectordb_config
    )
    qdrant_manager = qdrant_handler.get_qdrant_manager()
    return qdrant_manager


class Retriever:
    def __init__(
        self,
        embeddings_model: str,
        embeddings_collection_type: str,
        metadata_collection_type: str,
        top_k: int,
        top_n: int,
    ):
        self.article_qdrant_manager = initialize_article_qdrant_manager(
            embeddings_collection_type
        )
        self.metadata_qdrant_manager = initialize_metadata_qdrant_manager(
            metadata_collection_type
        )
        self.embeddings_model = embeddings_model
        self.top_k = top_k
        self.top_n = top_n
        llm_factory = LLMFactory()
        llm_handler = llm_factory.create_llm(llm_type="BedrockClaude")
        self.query_llm = llm_handler.get_query_llm()
        self.prompt_builder = PromptBuilder()

    def get_user_query_embeddings(self, user_query: str):
        model_info = get_model_info(self.embeddings_model)
        # Get embeddings for the user query
        query_vector = get_embeddings(
            model_name=model_info[0], token_limit=model_info[1], texts=[user_query]
        )
        return query_vector

    def retrieve_chunks(self, query_vector):
        # Search across chunks, retrieve a larger set to ensure diversity
        retrieved_chunks = self.article_qdrant_manager.search_vectors(
            query_vector=query_vector,
            limit=50,  # Fetch a higher number to ensure we meet distinct article criteria
        )

        # Collect chunks by article_id and ensure we have chunks from at least N distinct articles
        chunks_by_article = {}
        for chunk in retrieved_chunks:
            article_id = chunk.payload["article_id"]
            if article_id not in chunks_by_article:
                chunks_by_article[article_id] = []
            if len(chunks_by_article[article_id]) < self.top_k:
                chunks_by_article[article_id].append(chunk)

            # Stop if we've accumulated at least N distinct articles with chunks
            if len(chunks_by_article) >= self.top_n:
                break

        return chunks_by_article

    def get_llm_response_prompt(self, user_query: str, relevant_chunks: List[str]):
        llm_prompt = self.prompt_builder.get_llm_response_prompt(
            user_query=user_query,
            relevant_chunks=relevant_chunks,
        )
        logger.info(f"Generated prompt: {llm_prompt}")
        return llm_prompt

    def generate_results_from_llm(self, user_query: str, relevant_chunks: List[str]):
        # Generate the prompt
        llm_prompt = self.prompt_builder.get_llm_response_prompt(
            user_query=user_query, relevant_chunks=relevant_chunks
        )

        # Generate the response using Query LLM
        llm_summary_response = self.query_llm.invoke(input=llm_prompt)
        print(llm_summary_response)
        return llm_summary_response

    def get_article_ids_filtered_by_metadata(
        self, payload_filter: Dict[str, str]
    ) -> List[str]:
        """
        Filter article IDs based on metadata criteria.

        Args:
            retrieved_article_ids (List[str]): List of article IDs from initial similarity-based retrieval.
            payload_filter (Dict[str, str]): Metadata filter criteria as key-value pairs.

        Returns:
            List[str]: List of article IDs that match the metadata filter criteria.
        """
        # Fetch points matching the payload filter from the metadata collection
        matching_points = self.metadata_qdrant_manager.fetch_points_by_payload(
            payload_filter, limit=5000
        )

        # Extract IDs of articles that meet the metadata filter criteria
        matching_article_ids = {point["payload"]["pmcid"] for point in matching_points}
        # print(f"Matching article ids: {matching_article_ids}")

        return matching_article_ids

    def retrieve(self, query_vector, metadata_filters):
        # Step 1: Retrieve chunks ensuring at least N distinct articles
        chunks_by_article = self.retrieve_chunks(query_vector)

        # Get article IDs of retrieved chunks
        article_ids_from_similarity = [
            aid.split("_")[1] for aid in list(chunks_by_article.keys())
        ]
        print(f"Article IDs from similarity: {article_ids_from_similarity}")

        if len(metadata_filters) > 0:
            # Step 2: Filter articles by metadata criteria
            article_ids_from_metadata = self.get_article_ids_filtered_by_metadata(
                metadata_filters
            )
            print(f"Article IDs from metadata: {article_ids_from_metadata}")

            # Step 3: Take intersection of filtered article IDs
            final_article_ids = [
                "PMC_" + aid
                for aid in list(
                    set(article_ids_from_similarity) & set(article_ids_from_metadata)
                )
            ]
            print(f"Matching Articles Ids: {final_article_ids}")
        else:
            final_article_ids = ["PMC_" + aid for aid in article_ids_from_similarity]
            print(f"Matching Articles Ids: {final_article_ids}")

        # Filter the chunks to keep only those from articles passing metadata criteria
        # final_chunks_by_article = {aid: chunks_by_article[aid] for aid in final_article_ids}

        final_chunks_by_article = {
            aid: chunks_by_article[aid] for aid in final_article_ids
        }
        return final_chunks_by_article

    def parse_results(self, user_query, result):
        parsed_output = []

        for article_id, points in result.items():
            # parsed_output[article_id] = []

            for point in points:
                payload = point.payload  # Access as attribute instead of dict key

                # Extract the desired fields from the payload
                entry = {
                    "user_query": user_query,
                    "article_id": article_id,
                    "chunk_id": payload["chunk_id"],
                    "chunk_text": payload["chunk_text"],
                    "chunk_score": point.score,  # Access score as attribute
                }

                # entry = [
                #     user_query,
                #     article_id,
                #     payload["chunk_id"],
                #     payload["chunk_text"],
                #     point.score,
                # ]
                parsed_output.append(entry)

        return parsed_output

    def process_results_with_llm(self, result):
        """
        Process the result variable and generate LLM responses based on the selected approach.

        Args:
            result (list): List of dictionaries containing retrieved chunks.

        Returns:
            list: Updated results with LLM responses appended.
        """
        # Group chunks by article ID
        chunks_by_article = defaultdict(list)
        for entry in result:
            article_id = entry["article_id"]
            chunks_by_article[article_id].append(entry)

        updated_results = []

        for article_id, chunks in chunks_by_article.items():
            # Collect all chunk texts for the article and generate the per-article response
            chunks_text_list = [chunk["chunk_text"] for chunk in chunks]
            article_response = self.generate_results_from_llm(
                user_query=chunks[0]["user_query"], relevant_chunks=chunks_text_list
            ).content

            for chunk in chunks:
                # Generate the per-chunk response
                chunk_text = chunk["chunk_text"]
                chunk_response = self.generate_results_from_llm(
                    user_query=chunk["user_query"], relevant_chunks=[chunk_text]
                ).content

                # Add both responses to the chunk
                updated_chunk = chunk.copy()  # Avoid modifying the original data
                updated_chunk["article_response"] = article_response
                updated_chunk["chunk_response"] = chunk_response

                updated_results.append(updated_chunk)

        return updated_results


def flatten_list(nested_list):
    return [item for sublist in nested_list for item in sublist]


def run(run_type: str = "processed"):
    print("Runtype:", run_type)
    if run_type == "processed":
        output_path = "../../../data/results/processed/without_filter"
        results_file_path = (
            "../../../data/results/negetive_queries_processed_results.csv"
        )
        retriever = Retriever(
            embeddings_model="pubmedbert",
            embeddings_collection_type="processed_pubmedbert",
            metadata_collection_type="metadata",
            top_k=5,
            top_n=3,
        )
    elif run_type == "baseline":
        output_path = "../../../data/results/baseline/without_filter"
        results_file_path = (
            "../../../data/results/negetive_queries_baseline_results.csv"
        )
        retriever = Retriever(
            embeddings_model="pubmedbert",
            embeddings_collection_type="baseline",
            metadata_collection_type="metadata",
            top_k=5,
            top_n=3,
        )

    # Example query
    # user_queries = [
    #     "Effect of PM2.5 in EGFR mutation in lung cancer",
    #     "PI3K/AKT/mTOR and therapy resistance",
    # ]

    # # Actual Run
    # user_queries = [
    #     "Effect of PM2.5 in EGFR mutation in lung cancer",
    #     "PI3K/AKT/mTOR and therapy resistance",
    #     "PD-1 or PD-L1 biomarker and  lung cancer",
    #     "ScRNA seq with immune cell signatures and lung cancer",
    #     "ROS1 and lung cancer",
    #     "ctDNA and lung cancer",
    #     "mTOR/p70S6K/S6 pathway and breast cancer",
    #     "Caspase 3, Erk1/2 and Ovarian cancer",
    #     "TSC1/2, AMPK and Lung Cancer",
    #     "AKT-AMPK crosstalk and Diabetes",
    #     "EGFR, MET activation and Lung Cancer",
    #     "Rho activation and Ovarian cancer",
    #     "p27 kip and Ovarian cancer",
    #     "PI3K/AKT and Diabetes",
    #     "Nrf2 and lung cancer",
    #     "PI3K/AKT and lung cancer",
    #     "TGF b and Liver cancer",
    #     "p38 MAPK and diabetes",
    #     "IL2, VEGFR2 and Ovarian cancer",
    #     "TRAIL and obesity",
    #     "PI3/AKT and Ovarian cancer",
    #     "Haptoglobin and renal disease",
    #     "PI3K/AKT and obesity",
    #     "eNOS, VEGFR and Colon cancer",
    #     "Bcl6 lung cancer",
    #     "Ang II and obesity",
    #     "Erk and Obesity",
    #     "KRAS and lung cancer",
    #     "PI3K and obesity",
    #     "EGFR and lung cancer",
    #     "Akt1 and muscle hypertrophy",
    #     "CD24 and breast cancer",
    #     "ROS1 and lung cancer",
    #     "ALK4 and lung cancer",
    #     "EGFR and cervical lymphadenopathy",
    #     "PTEN and lung cancer",
    #     "ADAM17 and breast cancer",
    #     "VEGF and ovarian cancer",
    #     "AKT and diabetes",
    #     "PI3KR1 and breast cancer",
    #     "ERK1/2 and ovarian cancer",
    #     "ACE and diabetes",
    #     "Cyclin E and Ovarian cancer",
    #     "CD19 and DLBC",
    #     "CD138 and multiple myeloma",
    #     "MMP11 and foot ulcer",
    #     "CD44 and NSCLC",
    #     "PD-1 and NSCLC",
    #     "CTLA4 and non hodgkin lymphoma",
    #     "IL33 and COPD",
    #     "Lymphoma and RHOA",
    #     "Trem2 and retinal degeneration",
    #     "CD4 and infection",
    #     "BCL2/BCL6 and DLBCL",
    #     "Multiple myeloma and TNFSF13B",
    #     "Spinal cord development and CRABP1",
    #     "NSCLC and SP263",
    #     "B cell lymphoma and CREBP",
    #     "Cxcl2 and neurological disorders",
    #     "TIM3 and lung cancer",
    #     "STAT5B and leukemia",
    #     "Lrp1 and obesity",
    #     "PKM2 and retinopathy",
    #     "Serpina3n and inflammation",
    #     "HSP70AA1 and parkinsons disease",
    # ]

    # user_queries = [
    #     "How does the phosphorylation levels of p70S6K(T389) differ between cancerous and non-cancerous breast cancer cell lines?",
    #     "What is the effect of NVP-BKM120 on insulin levels in WT and MKR mice compared to vehicle-treated controls?",
    #     "What was the change in DRI for Skov3 and OV2008  cells upon treatment with Mifeprestone. Which of these cell lines had a higher DRI?",
    #     "How does obesity affect  p55α and p50α levels?",
    #     "Which VOC is found at higher concentration in HBEC-3kt53 cells and why?",
    #     "What are the reported specificity and sensitivity values of US±FNA for neck cancer detection in intermediate/high prevalence populations?",
    #     "What is the glucose metabolism phenotype of SIRT6BAC mice ?",
    #     "List the  cytokines that are in increased in the HFD mice?",
    #     "How does XCR1 expression correlate with estrogen receptor (ER) status in breast cancer, and how does this association differ from the reported ER-responsiveness of its ligand, XCL1?",
    #     "How does LDP compare to metformin in inhibiting IGF-1/IGF-1R/AKT signaling in gastric dysplasia of diabetic mice?",
    #     "How does ILQ affect the migration and proliferation of SKOV3 and OVCAR3 ovarian cancer cells in vitro, as assessed by wound healing and transwell assays?",
    #     "How does the miR-200/SUZ12/E-cadherin axis regulate epithelial-mesenchymal transition (EMT) and metastasis in breast cancer stem cells (BCSCs)?",
    #     "What is the diagnostic work up for a 47 year old patient with hemosputum?",
    #     "What was the percent decrease in migration of MDA-MB-231 cells upon treatment with oxymatrine?",
    #     "What is the implications of high SHOC2 levels in patients with breast cancer along with its significance in ER negative patients?",
    #     "Which CDH23 isoforms are capable of localizing to the stereocillia?",
    #     "Deleting which set of amino acids from C. elegans protein COSA-1 would most likely affect the ability of COSA-1 to recruit MSH5 and ZHP3?",
    #     "What is the structural change in the protein conformational ensemble from the A456V mutation in Human Glucokinase that accelerates glucose binding?",
    #     "How many putative G4-forming sequences are located within the human gene TMPRSS2?",
    #     "Approximately what percentage of adr-1(-), adr-2(-), and adr-1(-);adr-2(-) mutant C. elegans will die after exposure to 36ºC for 6 hours, where survival is assessed after 14h of recovery at 20ºC?",
    #     "By what factor did T cells with a anti-CD19 synNotch -> sIL-2 receptor circut expand within a mouse tumor?",
    #     "What is the measured dissociation constant for the Wnt5b-Ror2 complex in cytonemes of zebrafish?",
    #     "Which three residues with evolutionary divergence in the G domains of RAS isoforms also impose selectivity constraints of pan-KRAS non-covalent inhibition?",
    #     "For the channelrhodopsin found in Hyphochytrium catenoides (HcKCR1), the homology based structure predicted by ColabFold has a poor prediction for which one of the following transmembrane helices out of the 7 seven transmembrane helices in the structure ?",
    #     "Grafting ECL3 region from adenosine A3 receptor A3AR onto A2AAR does what to the efficacy of binding to the A3AR antagonist CF101 ?",
    #     "In Arabidopsis, which of the following 20 S proteasome subunits has CWC15 not been shown to interact with in its role promoting degradation of the protein Serrate?",
    #     "How does the chromatin occupancy of rTetR-VP48 change when you inhibit the cofactor P300?",
    # ]

    user_queries = [
        "How many differential H3K27Ac peaks are there between queen and worker honeybees?",
        "What is the mechanism by which Nrf1 protect the heart from Ischemia/Reperfusion injury?",
        # "For the cavity above p-hydroxybenzylidene moiety of the chromophore found in mSandy2 is filled by which rotamers adopted by Leucine found at position 63?",
        # "How many genes show changes in 5mC methylation of their promoter regions in Alzheimer's patients at Braak stages V/VI, compared to control?",
        # "Which RNA large language model accurately predicts 3D structures from a string of RNA sequences?",
        # "What are the top 5 immunogenicity neo antigens on cancer cells predicted by NeoDISC?",
        # "Based on PRS, which are the genes strongly assocaited with metabolic syndrome in brain tissues?",
        # "What are the top Gen AI use cases that are piloted in the Pharma R&D organizations to drive scientific breakthroughs?",
        # "What percentage of the control population carry ϵ4 allele of the APOE gene related to Alzhemier's disease?",
        # "What is PHQ9 score and how does it act as a potential indicator of depression?",
        # "Among TANGLE and ABMIL which method works best for image classification?",
        # "Explain how MIRO works to analyze microscopic data?",
        # "What is the mechanism of action by which ravolizumab acts on the complement system in myasthenia gravis?",
        # "What gene expression is altered in fetal growth restriction due to polyamine deficiency?",
        # "What cell death pathway is trigggered in the hepatocytes of  Mdm2Hep mice?",
        # "Where does fission take place in the neurons of Fmr1 KO mice and what are the fission rates in axons and dendrites?",
        # "In the cell fate mapping experiments of Aplnr-CreERT2 mice which cell type contributes to CAV1+ arteries?",
        # "Which part of the Arf1 component in the TGN acts as a site for non endocytic clathrin assembly?",
        # "What is effect of PGS1 knockdown on cardioplin in HEK293T cells using PRM-SRS microscopic method?",
        # "What are the different types of microscopy data formats that Vitessce supports?",
        # "What are the performance metrics of the algorithms submitted for the PANDA challenge?",
        # "How does clonal evoluation and tumor cell profileration change in the tumor microenviroment and how this impacts subclones?",
        # "Describe the association between cilia and MVB-derived smEVs",
        # "What are the properties of slender collagen fibers and its relevance in forming fibrous networks in the process of biopolymer gels?",
    ]

    final_result = []

    for index, user_query in enumerate(user_queries):
        logger.info(f"Processing User Query: {user_query}")
        query_vector = retriever.get_user_query_embeddings(user_query)[0]

        # Example metadata filters
        metadata_filters = {
            # "journal": "Nature",
            # "year": "2023"
        }

        retrieved_chunks = retriever.retrieve(query_vector, metadata_filters)
        result = retriever.parse_results(user_query, retrieved_chunks)
        llm_result = retriever.process_results_with_llm(result)
        # print(llm_result)
        final_result.append([list(d.values()) for d in llm_result])

    # Flatten the list of results
    final_result = flatten_list(final_result)
    print(final_result)

    # Write final_result to a csv
    headers = [
        "User Query",
        "Article ID",
        "Chunk ID",
        "Chunk Text",
        "Score",
        "Article Level LLM Response",
        "Chunk Level LLM Response",
    ]

    # with open(results_file_path, "w") as file:
    #     writer = csv.writer(file)
    #     writer.writerow(headers)
    #     writer.writerows(final_result)

    # result_file = f"{index}_result.json"
    # result_file_path = os.path.join(output_path, result_file)
    #
    # # Write the parsed result to a JSON file
    # with open(result_file_path, "w") as json_file:
    #     json.dump(result, json_file, indent=4)


if __name__ == "__main__":
    run_type = "processed"
    # run_type = "baseline"
    run(run_type=run_type)
