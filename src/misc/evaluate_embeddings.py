import json
import os
from src.data_processing.indexing.embeddings_handler import (
    get_embeddings,
    get_model_info,
    save_embeddings_details_to_json,
    load_embeddings_details_from_json,
    find_most_similar
)
import numpy as np
from typing import List, Union, Dict, Any

article_summary = """
Air pollution promotes lung cancer by inducing inflammation and expanding pre-existing oncogenic mutations. Particulate matter (PM2.5) exposure correlates with increased EGFR-driven lung cancer incidence across countries. PM2.5 triggers macrophage-derived interleukin-1β release, promoting a progenitor-like state in EGFR-mutant lung cells and accelerating tumor formation. Oncogenic EGFR and KRAS mutations were found in 18% and 53% of healthy lung samples, respectively, suggesting air pollutants may promote expansion of existing mutant clones rather than directly causing mutations.
"""

def generate_embedding_details():
    annotations_models = [
        "bioformer",
        "pubmedbert"
    ]

    embedding_models = [
        "bio_bert",
        "bio_gpt",
        "longformer",
        "sci_bert"
    ]

    all_embedding_detials = []

    chunk_dir = "../../data/PMC_7614604_chunks"
    #chunk_dir = "../../data/test/test"

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
                        f"Summary:\n{article_summary}\nText:\n{chunk["merged_text"]}"
                        for chunk in chunks
                    ]
                    #print(merged_texts_with_sum)
                    merged_texts_without_sum = [chunk["merged_text"] for chunk in chunks]

                    # Creating Embeddings with Summary
                    for embedding_model in embedding_models:
                        print(f"Processing for Embedding Model: {embedding_model} with article summary")
                        model_info = get_model_info(embedding_model)
                        embeddings = get_embeddings(
                            model_name=model_info[0],
                            token_limit=model_info[1],
                            texts=merged_texts_with_sum
                        )

                        embeddings_details = {
                            "file": cur_file,
                            "chunks_count": len(merged_texts_with_sum),
                            "annotation_model": annotation_model,
                            "embeddings_model": embedding_model,
                            "embeddings_model_token_limit": model_info[1],
                            "contains_summary": True,
                            "embeddings": embeddings
                        }

                        all_embedding_detials.append(embeddings_details)

                    # Creating Embeddings without Summary
                    for embedding_model in embedding_models:
                        print(f"Processing for Embedding Model: {embedding_model} with article summary")
                        model_info = get_model_info(embedding_model)
                        embeddings = get_embeddings(
                            model_name=model_info[0],
                            token_limit=model_info[1],
                            texts=merged_texts_without_sum
                        )

                        embeddings_details = {
                            "file": cur_file,
                            "chunks_count": len(merged_texts_without_sum),
                            "annotation_model": annotation_model,
                            "embeddings_model": embedding_model,
                            "embeddings_model_token_limit": model_info[1],
                            "contains_summary": False,
                            "embeddings": embeddings
                        }

                        all_embedding_detials.append(embeddings_details)

    # Write the Embeddings to a file:
    file_path = "../../data/PMC_7614604_chunks/embeddings/PMC_7614604_embeddings.json"
    save_embeddings_details_to_json(all_embedding_detials, file_path)


# Run the main function
if __name__ == "__main__":

    # Generate Embeddings:
    #generate_embedding_details()

    user_queries = [
        "lung cancer risk from air pollution",
    ]

    embedding_models = [
        "bio_bert",
        "bio_gpt",
        "longformer",
        "sci_bert"
    ]

    all_embedding_detials = load_embeddings_details_from_json(
        filename="../../data/PMC_7614604_chunks/embeddings/PMC_7614604_embeddings.json"
    )

    for user_query in user_queries:
        for embedding_model in embedding_models:
            print(f"Processing for model - {embedding_model}")
            model_info = get_model_info(embedding_model)
            query_embeddings = get_embeddings(
                model_name=model_info[0],
                token_limit=model_info[1],
                texts=[user_query]
            )


            # Find the most similar chunk:
            results = find_most_similar(
                query_embedding=query_embeddings,
                embeddings_list=all_embedding_detials,
                model=embedding_model,
                top_k=5
            )
            print(results)

# 1 Collection = Annotation Model X Embed Model X Chunking Strategy X Annotation Placement Strategy X Summ/No Summ X Each Chunk
# Total = 2 X 4 X 2 X 3 X 2 X Chunks = 96 X Chunks
#
# abx
# [aab, bbx, ccf, ddr]
# [0.89, 0.84, 0.14, 0.26]
#
# [0.91, 0.93, 0.14, 0.26]
#
# [0.87, 0.23, 0.14, 0.26]
#
# [0.75, 0.23, 0.14, 0.26]
#
#

