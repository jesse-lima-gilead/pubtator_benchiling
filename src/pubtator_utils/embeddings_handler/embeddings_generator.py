import os

import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel
import json
from typing import List
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()
model_path_config = config_loader.get_config("paths")["model"]["embeddings_model"]

# Getting the current dir path
current_dir = os.path.dirname(os.path.abspath(__file__))

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def get_model_info(model_name: str):
    try:
        model_path = model_path_config[model_name]["model_path"]
        token_limit = model_path_config[model_name]["token_limit"]
        logger.info(f"Model Loaded from {model_path}")
        return model_path, token_limit
    except Exception as e:
        raise ValueError(f"Error loading model {model_name}: {e}")


def masked_mean_pooling(token_embeddings, attention_mask):
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
    valid_token_count = input_mask_expanded.sum(dim=1).clamp(min=1e-9)
    mean_embeddings = sum_embeddings / valid_token_count
    return mean_embeddings


def get_embeddings(model_name, texts: List[str], stride=None):
    model_path, token_limit = get_model_info(model_name)
    logger.info(f"Model Path: {model_path} Model Token Limit: {token_limit}")
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = AutoModel.from_pretrained(model_path)

    max_length = token_limit
    stride = stride or max_length // 2

    all_embeddings = []

    for text in texts:
        tokens = tokenizer.encode(text, add_special_tokens=False)

        if len(tokens) <= max_length:
            # print("tokens: ", len(tokens), "small")
            inputs = tokenizer(
                text,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=max_length,
            )
            with torch.no_grad():
                outputs = model(**inputs)
            embeddings = masked_mean_pooling(
                outputs.last_hidden_state, inputs["attention_mask"]
            )
        else:
            # print("tokens: ", len(tokens), "large")
            chunk_embeddings = []
            chunk_masks = []
            for i in range(0, len(tokens), stride):
                chunk_tokens = tokens[i : i + max_length]

                # Convert tokens back to text
                chunk_text = tokenizer.decode(chunk_tokens)
                # Tokenize again with special tokens and padding
                chunk_inputs = tokenizer(
                    chunk_text,
                    return_tensors="pt",
                    padding="max_length",
                    truncation=True,
                    max_length=max_length,
                )

                with torch.no_grad():
                    chunk_outputs = model(**chunk_inputs)
                chunk_embeddings.append(chunk_outputs.last_hidden_state)
                chunk_masks.append(chunk_inputs["attention_mask"])

            #         # Combine chunk embeddings using masked mean pooling
            #         combined_embeddings = torch.cat(chunk_embeddings, dim=0)  # Concatenate along batch dimension
            #         combined_mask = torch.cat(chunk_masks, dim=0)  # Concatenate along batch dimension
            #         embeddings = masked_mean_pooling(combined_embeddings, combined_mask)
            #
            #     # Normalize embeddings
            #     embeddings = F.normalize(embeddings, p=2, dim=1)
            #     embeddings = embeddings.view(embeddings.size(0), -1)  # Flatten to 1D vector
            #
            #     all_embeddings.append(embeddings)
            #
            # # Stack all embeddings
            # return torch.cat(all_embeddings, dim=0)

            # Combine chunk embeddings using masked mean pooling
            combined_embeddings = torch.cat(chunk_embeddings, dim=1)
            combined_mask = torch.cat(chunk_masks, dim=1)
            embeddings = masked_mean_pooling(combined_embeddings, combined_mask)

        # Normalize embeddings
        embeddings = F.normalize(embeddings, p=2, dim=1)
        all_embeddings.append(embeddings)

    # Stack all embeddings
    return torch.cat(all_embeddings, dim=0)


def tensor_to_list(item):
    """Convert tensors and other non-serializable objects to lists."""
    if isinstance(item, torch.Tensor):  # Check if item is a PyTorch tensor
        return item.tolist()  # Convert tensor to list
    elif isinstance(item, list):
        return [
            tensor_to_list(sub_item) for sub_item in item
        ]  # Recursively convert lists
    elif isinstance(item, dict):
        return {
            key: tensor_to_list(value) for key, value in item.items()
        }  # Recursively convert dicts
    else:
        return item  # Return the item if it's already serializable


def save_embeddings_details_to_json(
    embeddings_details_list, filename, file_handler, s3_filename, s3_file_handler
):
    """Convert each item in the embeddings details list to JSON-serializable format and save to a JSON file."""
    # Ensure we process each dictionary in the list to make it JSON-serializable
    serializable_data = []
    for item in embeddings_details_list:
        if isinstance(item, dict):
            serializable_data.append(tensor_to_list(item))
        else:
            raise ValueError(
                "Each item in embeddings_details_list should be a dictionary."
            )

    # Write the JSON-serializable data to a file
    # with open(filename, "w") as f:
    #     json.dump(serializable_data, f, indent=2)

    file_handler.write_file_as_json(filename, serializable_data)
    s3_file_handler.write_file_as_json(s3_filename, serializable_data)


def load_embeddings_details_from_json(filename):
    with open(filename, "r") as f:
        return json.load(f)


# Function to calculate cosine similarity between query embedding and chunk embeddings
def calculate_similarity(query_embedding, chunk_embeddings):
    query_embedding = np.array(query_embedding).reshape(1, -1)
    chunk_embeddings = np.array(chunk_embeddings)
    return cosine_similarity(query_embedding, chunk_embeddings).flatten()


def find_most_similar(
    user_query, query_embedding, embeddings_details, model, article_id, top_k=5
):
    results = []

    # Find the item with the matching model and extract its embeddings
    for item in embeddings_details:
        if item["embeddings_model"] == model:
            file = item.get("file")
            embedding_model = item.get("embeddings_model")
            annotation_model = item.get("annotation_model")
            contains_summary = item.get("contains_summary")
            chunking_strategy = (
                "grouped_annotation_aware_sliding_window"
                if "grouped_annotation_aware_sliding_window" in file
                else (
                    "sliding_window"
                    if "sliding_window" in file
                    else (
                        "annotation_aware"
                        if "annotation_aware" in file
                        else "passage"
                        if "passage" in file
                        else "none"
                    )
                )
            )
            annotations_placement_strategy = (
                "append"
                if "append" in file
                else (
                    "prepend"
                    if "prepend" in file
                    else "inline"
                    if "inline" in file
                    else "none"
                )
            )
            embeddings = item.get("embeddings")

            # Calculate similarities
            similarities = calculate_similarity(query_embedding, embeddings)

            # Get top k indices based on similarity scores
            top_indices = np.argsort(similarities)[::-1][:top_k]
            # print(top_indices, "top indices")

            for idx in top_indices:
                results.append(
                    {
                        "pmc_article_id": article_id,
                        "User Query": user_query,
                        "Embed Model": embedding_model,
                        "Annotation Model": annotation_model,
                        "Chunking Strategy": chunking_strategy,
                        "Annotation Placement Strategy": annotations_placement_strategy,
                        "Contains Summary": "Yes" if contains_summary else "No",
                        "Chunk Sequence": f"c{idx + 1}",
                        "Similarity Score": similarities[idx],
                        "Chunk File": file,
                    }
                )

                # # Insert into PostgreSQL
                # similarity_record = ChunkSimilarity(
                #     user_query=user_query,
                #     embed_model=embedding_model,
                #     annotation_model=annotation_model,
                #     chunking_strategy=chunking_strategy,
                #     annotation_placement_strategy=annotations_placement_strategy,
                #     contains_summary="Yes" if contains_summary else "No",
                #     chunk_sequence=f"c{idx + 1}",
                #     similarity_score=similarities[idx],
                #     chunk_file=file
                # )
                # session.add(similarity_record)
                # session.commit()

    return results


# Save results to CSV
# def save_to_csv(results, output_file):
#     # Create the directory if it doesn't exist
#     os.makedirs(os.path.dirname(output_file), exist_ok=True)
#     df = pd.DataFrame(results)
#     df.to_csv(output_file, index=False)


def save_to_csv(results, output_file):
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Convert results to a DataFrame
    df = pd.DataFrame(results)

    # Check if the file already exists
    if os.path.isfile(output_file):
        # Append without writing the header
        df.to_csv(output_file, mode="a", header=False, index=False)
    else:
        # Write normally if file doesn't exist
        df.to_csv(output_file, mode="w", index=False)


if __name__ == "__main__":
    model_name = "pubmedbert"
    # model_info = get_model_info(model_name)

    ### Checking for Creating Chunks Embeddings:

    # chunk_caps = """
    # Summary:
    # Air pollution promotes lung cancer by inducing inflammation and expanding pre-existing oncogenic mutations. Particulate matter (PM2.5) exposure correlates with increased EGFR-driven lung cancer incidence across countries. PM2.5 triggers macrophage-derived interleukin-1β release, promoting a progenitor-like state in EGFR-mutant lung cells and accelerating tumor formation. Oncogenic EGFR and KRAS mutations were found in 18% and 53% of healthy lung samples, respectively, suggesting air pollutants may promote expansion of existing mutant clones rather than directly causing mutations.
    #
    # Annotations:
    # Text - GFR
    # Type - Gene
    # NCBI Gene - 13649
    # Text - RAS
    # Type - Gene
    # NCBI Gene - 16653
    # Text - mouse
    # Type - Species
    # NCBI Taxonomy - 10090
    # Text - EGFR
    # Type - Gene
    # NCBI Gene - 13649
    # Text - KRAS
    # Type - Gene
    # NCBI Gene - 16653
    #
    # Text:
    # A complete understanding of how exposure to environmental substances promotes cancer formation is lacking. More than 70 years ago, tumorigenesis was proposed to occur in a two-step process: an initiating step that induces mutations in healthy cells, followed by a promoter step that triggers cancer development1. Here we propose that environmental particulate matter measuring ≤2.5 μm (PM2.5), known to be associated with lung cancer risk, promotes lung cancer by acting on cells that harbour pre-existing oncogenic mutations in healthy lung tissue. Focusing on EGFR-driven lung cancer, which is more common in never-smokers or light smokers, we found a significant association between PM2.5 levels and the incidence of lung cancer for 32,957 EGFR driven lung cancer cases in four within-country cohorts. Functional mouse models revealed that air pollutants cause an influx of macrophages into the lung and release of interleukin-1β. This process results in a progenitor-like cell state within EGFR mutant lung alveolar type II epithelial cells that fuels tumorigenesis. Ultradeep mutational profiling of histologically normal lung tissue from 295 individuals across 3 clinical cohorts revealed oncogenic EGFR and KRAS driver mutations in 18% and 53% of healthy tissue samples, respectively. These findings collectively support a tumour promoting role for PM2.5 air pollutants and provide impetus for public health policy initiatives to address air pollution to reduce disease burden.
    # """
    #
    # # Example usage:
    # embeddings_details_list = [
    #     {
    #         "file": "document1.txt",
    #         "chunks_count": 5,
    #         "annotation_model": "GPT-3",
    #         "embeddings_model": "BERT",
    #         "embeddings_model_token_limit": 512,
    #         "contains_summary": False,
    #         "embeddings": get_embeddings(
    #             model_name=model_info[0], token_limit=model_info[1], texts=[chunk_caps]
    #         ),
    #     },
    #     {
    #         "file": "document2.txt",
    #         "chunks_count": 3,
    #         "annotation_model": "GPT-3",
    #         "embeddings_model": "BERT",
    #         "embeddings_model_token_limit": 512,
    #         "contains_summary": False,
    #         "embeddings": get_embeddings(
    #             model_name=model_info[0], token_limit=model_info[1], texts=[chunk_caps]
    #         ),
    #     },
    # ]
    #
    # # embeddings = get_embeddings(
    # #     model_name=model_info[0],
    # #     token_limit=model_info[1],
    # #     texts=[chunk_caps]
    # # )
    #
    # print(embeddings_details_list)

    # save_embeddings_details_to_json(embeddings_details_list, filename="embeddings3.json")

    # #### Checking For User Query:
    #
    # user_query = "lung cancer risk from air pollution"
    # query_embeddings = get_embeddings(
    #     model_name=model_name, texts=[user_query]
    # )
    # print(query_embeddings)

    ##### Doing a Simillarity Search
