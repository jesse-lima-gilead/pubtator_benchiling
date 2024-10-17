import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel
import json
from typing import List, Union, Dict, Any
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


def get_model_info(model_name: str):
    """Factory method to return the appropriate model info based on the model_name."""
    if model_name == "bio_bert":
        return "dmis-lab/biobert-v1.1", 512
    elif model_name == "bio_gpt":
        return "microsoft/biogpt", 1024
    elif model_name == "longformer":
        return "allenai/longformer-base-4096", 4096
    elif model_name == "big_berd":
        return "google/bigbird-roberta-base", 4096
    elif model_name == "sci_bert":
        return "allenai/scibert_scivocab_uncased", 512
    else:
        raise ValueError(f"Unknown model_name: {model_name}")


def masked_mean_pooling(token_embeddings, attention_mask):
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
    valid_token_count = input_mask_expanded.sum(dim=1).clamp(min=1e-9)
    mean_embeddings = sum_embeddings / valid_token_count
    return mean_embeddings


def get_embeddings(model_name, texts, token_limit=512, stride=None):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)

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

            # Combine chunk embeddings using masked mean pooling
            combined_embeddings = torch.cat(chunk_embeddings, dim=1)
            combined_mask = torch.cat(chunk_masks, dim=1)
            embeddings = masked_mean_pooling(combined_embeddings, combined_mask)

        # Normalize embeddings
        embeddings = F.normalize(embeddings, p=2, dim=1)
        all_embeddings.append(embeddings)

    # Stack all embeddings
    return torch.cat(all_embeddings, dim=0)


# def masked_mean_pooling(token_embeddings: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
#     """
#     Perform masked mean pooling on the token embeddings.
#
#     Args:
#         token_embeddings (torch.Tensor): The token embeddings from the model.
#         attention_mask (torch.Tensor): The attention mask for the input.
#
#     Returns:
#         torch.Tensor: The pooled embeddings.
#     """
#     input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
#     sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
#     sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
#     return sum_embeddings / sum_mask

# def get_embeddings(
#     model_name: str,
#     texts: List[str],
#     token_limit: int = 512,
#     stride: int = 256,
#     return_numpy: bool = False
# ) -> Union[torch.Tensor, np.ndarray]:
#     """
#     Generate embeddings for a list of texts using the specified model.
#
#     Args:
#         model_name (str): The name of the pre-trained model to use.
#         texts (List[str]): A list of texts to generate embeddings for.
#         token_limit (int, optional): The maximum number of tokens per chunk. Defaults to 512.
#         stride (int, optional): The stride for chunking long texts. Defaults to 256.
#         return_numpy (bool, optional): If True, return a numpy array instead of a PyTorch tensor. Defaults to False.
#
#     Returns:
#         Union[torch.Tensor, np.ndarray]: The generated embeddings.
#     """
#     tokenizer = AutoTokenizer.from_pretrained(model_name)
#     model = AutoModel.from_pretrained(model_name)
#
#     # Move model to GPU if available
#     device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
#     model.to(device)
#
#     all_embeddings = []
#
#     # Get the model's maximum sequence length
#     max_length = min(tokenizer.model_max_length, token_limit)
#     stride = stride or max_length // 2
#
#     for text in texts:
#         encoded_input = tokenizer.encode_plus(
#             text,
#             add_special_tokens=True,
#             max_length=max_length,
#             truncation=True,
#             padding='max_length',
#             return_tensors='pt'
#         )
#
#         encoded_input = {k: v.to(device) for k, v in encoded_input.items()}
#
#         with torch.no_grad():
#             model_output = model(**encoded_input)
#
#         # Use mean pooling
#         token_embeddings = model_output.last_hidden_state
#         attention_mask = encoded_input['attention_mask']
#         embeddings = masked_mean_pooling(token_embeddings, attention_mask)
#
#         # Normalize embeddings
#         embeddings = F.normalize(embeddings, p=2, dim=1)
#         all_embeddings.append(embeddings)
#
#     # Stack all embeddings
#     final_embeddings = torch.cat(all_embeddings, dim=0)
#
#     if return_numpy:
#         return final_embeddings.cpu().numpy()
#     return final_embeddings


#
# def get_embeddings(model_name, token_limit, text, stride=256):
#     # Load pre-trained model and tokenizer
#     tokenizer = AutoTokenizer.from_pretrained(model_name)
#     model = AutoModel.from_pretrained(model_name)
#
#     # Get the model's maximum sequence length
#     max_length = tokenizer.model_max_length if hasattr(tokenizer, 'model_max_length') else token_limit
#
#     # Tokenize the text without truncation first
#     tokens = tokenizer.encode(text, add_special_tokens=True)
#
#     # Split the tokens into chunks that fit within the model's max length
#     chunks = [tokens[i:i + max_length] for i in range(0, len(tokens), max_length)]
#
#     all_embeddings = []
#
#     for chunk in chunks:
#         # Convert chunk to tensor and reshape
#         input_ids = torch.tensor(chunk).unsqueeze(0)
#
#         # Generate embeddings
#         with torch.no_grad():
#             outputs = model(input_ids)
#
#         # Use the last hidden state as the embedding
#         embeddings = outputs.last_hidden_state.mean(dim=1)
#         all_embeddings.append(embeddings)
#
#     # Concatenate all embeddings if there were multiple chunks
#     final_embedding = torch.cat(all_embeddings, dim=0).mean(dim=0)
#
#     return final_embedding.numpy()


def tensor_to_list(obj):
    if isinstance(obj, (torch.Tensor, np.ndarray)):
        return (
            obj.detach().cpu().numpy().tolist()
            if isinstance(obj, torch.Tensor)
            else obj.tolist()
        )
    elif isinstance(obj, (list, tuple)):
        return [tensor_to_list(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: tensor_to_list(value) for key, value in obj.items()}
    else:
        return obj


def save_embeddings_details_to_json(embeddings_details_list, filename):
    serializable_data = [tensor_to_list(item) for item in embeddings_details_list]

    with open(filename, "w") as f:
        json.dump(serializable_data, f, indent=2)


def load_embeddings_details_from_json(filename):
    with open(filename, "r") as f:
        return json.load(f)


def find_most_similar(query_embedding, embeddings_list, model, top_k=5):
    all_embeddings = []
    all_files = []

    # Find the item with the matching model and extract its embeddings
    for item in embeddings_list:
        if item["embeddings_model"] == model:
            all_embeddings.extend(item["embeddings"])
            all_files.extend([item["file"]] * len(item["embeddings"]))

    if not all_embeddings:
        raise ValueError(f"No embeddings found for model: {model}")

    # Convert to numpy array for efficient computation
    all_embeddings = np.array(all_embeddings)

    # Ensure query_embedding is 2D
    if query_embedding.ndim == 1:
        query_embedding = query_embedding.reshape(1, -1)

    # Calculate cosine similarity
    similarities = cosine_similarity(query_embedding, all_embeddings)[0]

    # Get indices of top_k most similar embeddings
    most_similar_indices = similarities.argsort()[-top_k:][::-1]

    # Create result list
    results = []
    for idx in most_similar_indices:
        results.append(
            {
                "similarity": similarities[idx],
                "file": all_files[idx],
                "embedding": all_embeddings[idx].tolist(),
            }
        )

    return results


if __name__ == "__main__":
    model_name = "bio_bert"
    chunk_caps = """
    Summary:
    Air pollution promotes lung cancer by inducing inflammation and expanding pre-existing oncogenic mutations. Particulate matter (PM2.5) exposure correlates with increased EGFR-driven lung cancer incidence across countries. PM2.5 triggers macrophage-derived interleukin-1β release, promoting a progenitor-like state in EGFR-mutant lung cells and accelerating tumor formation. Oncogenic EGFR and KRAS mutations were found in 18% and 53% of healthy lung samples, respectively, suggesting air pollutants may promote expansion of existing mutant clones rather than directly causing mutations.

    Annotations:
    Text - GFR
    Type - Gene
    NCBI Gene - 13649
    Text - RAS
    Type - Gene
    NCBI Gene - 16653
    Text - mouse
    Type - Species
    NCBI Taxonomy - 10090
    Text - EGFR
    Type - Gene
    NCBI Gene - 13649
    Text - KRAS
    Type - Gene
    NCBI Gene - 16653

    Text:
    A complete understanding of how exposure to environmental substances promotes cancer formation is lacking. More than 70 years ago, tumorigenesis was proposed to occur in a two-step process: an initiating step that induces mutations in healthy cells, followed by a promoter step that triggers cancer development1. Here we propose that environmental particulate matter measuring ≤2.5 μm (PM2.5), known to be associated with lung cancer risk, promotes lung cancer by acting on cells that harbour pre-existing oncogenic mutations in healthy lung tissue. Focusing on EGFR-driven lung cancer, which is more common in never-smokers or light smokers, we found a significant association between PM2.5 levels and the incidence of lung cancer for 32,957 EGFR driven lung cancer cases in four within-country cohorts. Functional mouse models revealed that air pollutants cause an influx of macrophages into the lung and release of interleukin-1β. This process results in a progenitor-like cell state within EGFR mutant lung alveolar type II epithelial cells that fuels tumorigenesis. Ultradeep mutational profiling of histologically normal lung tissue from 295 individuals across 3 clinical cohorts revealed oncogenic EGFR and KRAS driver mutations in 18% and 53% of healthy tissue samples, respectively. These findings collectively support a tumour promoting role for PM2.5 air pollutants and provide impetus for public health policy initiatives to address air pollution to reduce disease burden.
    """
    model_info = get_model_info(model_name)
    # Example usage:
    embeddings_details_list = [
        {
            "file": "document1.txt",
            "chunks_count": 5,
            "annotation_model": "GPT-3",
            "embeddings_model": "BERT",
            "embeddings_model_token_limit": 512,
            "contains_summary": False,
            "embeddings": get_embeddings(
                model_name=model_info[0], token_limit=model_info[1], texts=[chunk_caps]
            ),
        },
        {
            "file": "document2.txt",
            "chunks_count": 3,
            "annotation_model": "GPT-3",
            "embeddings_model": "BERT",
            "embeddings_model_token_limit": 512,
            "contains_summary": False,
            "embeddings": get_embeddings(
                model_name=model_info[0], token_limit=model_info[1], texts=[chunk_caps]
            ),
        },
    ]

    # embeddings = get_embeddings(
    #     model_name=model_info[0],
    #     token_limit=model_info[1],
    #     texts=[chunk_caps]
    # )
    #
    # print(embeddings_details_list)
    #
    # save_embeddings_details_to_json(embeddings_details_list, filename="embeddings3.json")

    user_queries = [
        "lung cancer risk from air pollution",
    ]

    embedding_models = [
        "bio_bert",
        "bio_gpt",
        # "longformer",
        # "big_berd",
        # "sci_bert"
    ]

    all_embedding_detials = load_embeddings_details_from_json(
        filename="../../data/PMC_7614604_chunks/PMC_7614604_embeddings_backup.json"
    )

    for user_query in user_queries:
        for embedding_model in embedding_models:
            print(f"Processing for model - {embedding_model}")
            model_info = get_model_info(embedding_model)
            query_embeddings = get_embeddings(
                model_name=model_info[0], token_limit=model_info[1], texts=[user_query]
            )

            # Find the most similar chunk:
            results = find_most_similar(
                query_embedding=query_embeddings,
                embeddings_list=all_embedding_detials,
                model=embedding_model,
                top_k=3,
            )
            print(results)
