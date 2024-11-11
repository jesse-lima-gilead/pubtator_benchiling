import json
import os

from misc.evaluate_baseline_embeddings import article_summary
from src.data_processing.embedding.embeddings_handler import (
    get_embeddings,
    get_model_info,
    save_embeddings_details_to_json,
    load_embeddings_details_from_json,
    find_most_similar,
    save_to_csv,
)
import numpy as np
from typing import List, Union, Dict, Any

article_summary = """
Air pollution promotes lung cancer by inducing inflammation and expanding pre-existing oncogenic mutations. Particulate matter (PM2.5) exposure correlates with increased EGFR-driven lung cancer incidence across countries. PM2.5 triggers macrophage-derived interleukin-1β release, promoting a progenitor-like state in EGFR-mutant lung cells and accelerating tumor formation. Oncogenic EGFR and KRAS mutations were found in 18% and 53% of healthy lung samples, respectively, suggesting air pollutants may promote expansion of existing mutant clones rather than directly causing mutations.
"""


def extract_summary(filename):
    with open(filename, "r") as file:
        lines = file.readlines()

    # Skip the first line and join the rest into a single summary text
    summary = "".join(lines[1:]).strip()
    return summary


def generate_embedding_details(cur_pmc_dir):
    annotations_models = [
        "bioformer",
        # "pubmedbert"
    ]

    embedding_models = [
        # "bio_bert",
        # "bio_gpt",
        # "longformer",
        # "sci_bert",
        "medembed",
        "pubmedbert"
    ]

    all_embedding_detials = []

    chunk_dir = f"../../test_data/medembed_analysis/chunks/{cur_pmc_dir}"
    # chunk_dir = "../../data/test/test"
    article_file_path = f"../../test_data/medembed_analysis/article_summaries/{cur_pmc_dir}.txt"
    article_summary = extract_summary(article_file_path)
    print(article_summary)

    # Read the chunks and create embeddings:
    for annotation_model in annotations_models:
        print("Processing for Annotation Model: ", annotation_model)
        for cur_file in os.listdir(chunk_dir):
            if cur_file.endswith(".json") and annotation_model in cur_file:
                input_file_path = f"{chunk_dir}/{cur_file}"

                with open(f"{input_file_path}", "r") as f:
                    print(f"Processing {input_file_path}")
                    chunks = json.load(f)
                    merged_texts_with_sum = [
                        f"Summary:\n{article_summary}\nText:\n{chunk['merged_text']}"
                        for chunk in chunks
                    ]
                    # print(merged_texts_with_sum)
                    # merged_texts_without_sum = [chunk["merged_text"] for chunk in chunks]

                    # Creating Embeddings with Summary
                    for embedding_model in embedding_models:
                        print(
                            f"Processing for Embedding Model: {embedding_model} with article summary"
                        )
                        model_info = get_model_info(embedding_model)
                        embeddings = get_embeddings(
                            model_name=model_info[0],
                            token_limit=model_info[1],
                            texts=merged_texts_with_sum,
                        )

                        embeddings_details = {
                            "file": cur_file,
                            "chunks_count": len(merged_texts_with_sum),
                            "annotation_model": annotation_model,
                            "embeddings_model": embedding_model,
                            "embeddings_model_token_limit": model_info[1],
                            "contains_summary": True,
                            "embeddings": embeddings,
                        }

                        all_embedding_detials.append(embeddings_details)

                    # # Creating Embeddings without Summary
                    # for embedding_model in embedding_models:
                    #     print(f"Processing for Embedding Model: {embedding_model} without article summary")
                    #     model_info = get_model_info(embedding_model)
                    #     embeddings = get_embeddings(
                    #         model_name=model_info[0],
                    #         token_limit=model_info[1],
                    #         texts=merged_texts_without_sum
                    #     )
                    #
                    #     embeddings_details = {
                    #         "file": cur_file,
                    #         "chunks_count": len(merged_texts_without_sum),
                    #         "annotation_model": annotation_model,
                    #         "embeddings_model": embedding_model,
                    #         "embeddings_model_token_limit": model_info[1],
                    #         "contains_summary": False,
                    #         "embeddings": embeddings
                    #     }
                    #
                    #     all_embedding_detials.append(embeddings_details)

    # Write the Embeddings to a file:
    file_path = f"../../test_data/medembed_analysis/embeddings/{cur_pmc_dir}_embeddings.json"
    save_embeddings_details_to_json(all_embedding_detials, file_path)


# Run the main function
if __name__ == "__main__":
    chunk_dir = f"../../test_data/medembed_analysis/chunks"

    # cur_pmc_dir = "PMC_10213952"
    # generate_embedding_details(cur_pmc_dir)


    for cur_pmc_dir in os.listdir(chunk_dir):
        print(f'{chunk_dir}/{cur_pmc_dir}')
        # Generate Embeddings:
        generate_embedding_details(cur_pmc_dir)

    # user_queries = [
    #     # "lung cancer and air pollution",
    #     # "interleukin-1β in lung cancer progression",
    #     "Impact of Particulate Matter (PM2.5) on lung cancer with EGFR mutation",
    # ]

    # user_queries = [
    #     # "lung cancer and air pollution",
    #     # "interleukin-1β in lung cancer progression",
    #     "EGFR mutations and aging",
    #     "KRAS mutations in Korean population",
    #     "PM2.5 effects in England cohorts",
    #     "EGFR mutation frequency healthy lung tissue"
    # ]
    #
    # embedding_models = [
    #     "bio_bert",
    #     #"bio_gpt",
    #     "longformer",
    #     #"sci_bert"
    # ]
    #
    # # all_embedding_detials = load_embeddings_details_from_json(
    # #     filename="../../data/PMC_7614604_chunks/embeddings/PMC_7614604_embeddings.json"
    # # )
    # i = 0
    # for user_query in user_queries:
    #     print("user_query: ", user_query)
    #     for decoy_pmc_dir in os.listdir(chunk_dir):
    #         print("decoy_pmc_dir: ", decoy_pmc_dir)
    #         all_embedding_detials = load_embeddings_details_from_json(
    #             filename=f"../../data/decoy_docs/embeddings/{decoy_pmc_dir}_embeddings.json"
    #         )
    #         for embedding_model in embedding_models:
    #             print(f"Processing for model - {embedding_model}")
    #             model_info = get_model_info(embedding_model)
    #             query_embeddings = get_embeddings(
    #                 model_name=model_info[0],
    #                 token_limit=model_info[1],
    #                 texts=[user_query]
    #             )
    #
    #
    #             # Find the most similar chunk:
    #             results = find_most_similar(
    #                 user_query=user_query,
    #                 query_embedding=query_embeddings,
    #                 embeddings_details=all_embedding_detials,
    #                 model=embedding_model,
    #                 article_id=decoy_pmc_dir,
    #                 top_k=5
    #             )
    #
    #
    #             save_to_csv(
    #                 results=results,
    #                 # output_file=f"../../data/decoy_docs/similarity_results/que{i}/{decoy_pmc_dir}/{embedding_model}.csv"
    #                 output_file=f"../../data/decoy_docs/similarity_results/que{i}/results.csv"
    #             )
    #
    #     i += 1


# 1 Collection =
# No. of Articles X Annotation Model X Embed Model X Chunking Strategy X Annotation Placement Strategy X Summ/No Summ X Each Chunk
# Total = 1 X 2 X 4 X 2 X 3 X 2 X Chunks = 96 X Chunks
#
# Annotation Models - Bioformer, Pubmedbert  -> Bioformer
# Embedding Models - Bio_bert, Bio_gpt, Longformer, Sci_bert, PubmedBert -> Bio_bert and PubmedBert
# Chunking Strategies - Sliding Window, Annotation Aware Sliding Window, Grouped Annotation Aware Sliding Window, Passage -> Sliding Window
# Annotation Placement Strategies - Prepend, Inline, Append -> Prepend
# Summ/No Summ - Contains Summary, Doesn't Contain Summary -> Contains Summary