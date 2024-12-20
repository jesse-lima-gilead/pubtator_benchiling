import os
import json

from dotenv import load_dotenv
from qdrant_client import QdrantClient
import shutil

from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, FilterSelector

from src.utils.config_reader import YAMLConfigLoader


def count_points_by_article_id(client: QdrantClient, collection_name: str, article_id: str) -> int:
    """
    Count the number of points in a Qdrant collection for a specific article ID.

    Args:
        client (QdrantClient): Initialized Qdrant client.
        collection_name (str): Name of the Qdrant collection.
        article_id (str): The article ID to filter points.

    Returns:
        int: The number of points matching the filter.
    """
    # Define the filter
    filter_criteria = Filter(
        must=[
            FieldCondition(
                key="article_id",
                match=MatchValue(value=article_id)
            )
        ]
    )

    # Count the points
    response = client.count(
        collection_name=collection_name,
        count_filter=filter_criteria,
        exact=True  # Exact count
    )

    return response.count


def delete_points_by_article_id(client: QdrantClient, collection_name: str, article_id: str) -> None:
    """
    Delete points in a Qdrant collection for a specific article ID.

    Args:
        client (QdrantClient): Initialized Qdrant client.
        collection_name (str): Name of the Qdrant collection.
        article_id (str): The article ID to filter points for deletion.
    """

    # Perform deletion
    client.delete(
        collection_name=collection_name,
        points_selector=FilterSelector(
            filter=Filter(
                must=[
                    FieldCondition(
                        key="article_id",
                        match=MatchValue(value=article_id),
                    ),
                ],
            )
        ),
    )
    print(f"Deleted points with article_id: {article_id}")

def get_article_points(client: QdrantClient, collection_name: str) -> dict:
    """
    Get a dictionary of article IDs and the number of points for each from the Qdrant collection.

    Args:
        client (QdrantClient): Initialized Qdrant client.
        collection_name (str): Name of the collection.

    Returns:
        dict: A dictionary where keys are article IDs and values are point counts.
    """
    article_points_dic = {}
    next_page_token = None
    limit = 100  # Adjust based on performance needs
    total_points = 0

    while True:
        points, next_page_token = client.scroll(
            collection_name=collection_name,
            limit=limit,
            with_payload=True,
            offset=next_page_token,
        )
        total_points += len(points)
        for point in points:
            article_id = point.payload.get("article_id")
            if article_id:
                article_points_dic[article_id] = article_points_dic.get(article_id, 0) + 1

        if not next_page_token:
            break

    print(total_points, "total_points")
    return article_points_dic

def get_article_points_by_chunk_name(client: QdrantClient, collection_name: str) -> dict:
    """
    Get a dictionary of article IDs and the number of points for each from the Qdrant collection.

    Args:
        client (QdrantClient): Initialized Qdrant client.
        collection_name (str): Name of the collection.

    Returns:
        dict: A dictionary where keys are article IDs and values are point counts.
    """
    chunk_name_dic = {}
    next_page_token = None
    limit = 100  # Adjust based on performance needs

    while True:
        points, next_page_token = client.scroll(
            collection_name=collection_name,
            limit=limit,
            with_payload=True,
            offset=next_page_token,
        )

        for point in points:
            chunk_name = point.payload.get("chunk_name")
            if chunk_name:
                chunk_name_dic[chunk_name] = chunk_name_dic.get(chunk_name, 0) + 1

        if not next_page_token:
            break

    return chunk_name_dic

import shutil
import os
import json

def validate_article_points_and_move(article_points_dic: dict, source_dir: str, destination_dir: str) -> dict:
    """
    Validate if the number of points for each article matches the length of the list in its JSON file.
    If validation passes, move the JSON file from source_dir to destination_dir.

    Args:
        article_points_dic (dict): Dictionary of article IDs and their point counts.
        source_dir (str): Path to the directory containing JSON files.
        destination_dir (str): Path to the directory where valid files will be moved.

    Returns:
        dict: Validation results where keys are article IDs and values are True/False.
    """
    validation_results = {}

    # Ensure the destination directory exists
    os.makedirs(destination_dir, exist_ok=True)

    for article_id, point_count in article_points_dic.items():
        file_path = os.path.join(source_dir, f"{article_id}.json")
        destination_path = os.path.join(destination_dir, f"{article_id}.json")

        if not os.path.isfile(file_path):
            print(f"File not found for article_id: {article_id}")
            validation_results[article_id] = False
            continue

        with open(file_path, 'r') as file:
            try:
                article_data = json.load(file)
                list_length = len(article_data)
                is_valid = (list_length == point_count)
                validation_results[article_id] = is_valid

                # If valid, move the file to the destination directory
                if is_valid:
                    shutil.move(file_path, destination_path)
                    print(f"Moved valid file: {file_path} -> {destination_path}")
                else:
                    print("File found but mismatch records length",file_path)

            except json.JSONDecodeError:
                print(f"Error decoding JSON for file: {file_path}")
                validation_results[article_id] = False

    return validation_results

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
# # Docker Qdrant
# vectordb_config = config_loader.get_config("vectordb")["qdrant"]
# Cloud Qdrant
vectordb_config = config_loader.get_config("vectordb")["qdrant_cloud"]

load_dotenv()

if __name__ == "__main__":
    # Qdrant setup
    client = QdrantClient(url=vectordb_config['url'], api_key=os.environ.get('QDRANT_API_KEY'))  # Adjust host and port if needed
    collection_name = vectordb_config['collections']['baseline']['collection_name']

    # Directories
    source_dir = "../../data/litqa_dataset/indexing/chunks"  # Directory containing JSON files
    destination_dir = "../../data/litqa_dataset/indexing/processed_chunks"  # Directory to move valid files

    # Step 1: Get article points
    article_points_dic = get_article_points(client, collection_name)

    # Step 2: Validate points and move valid files
    validation_results = validate_article_points_and_move(article_points_dic, source_dir, destination_dir)

    # Print results
    for article_id, is_valid in validation_results.items():
        print(f"Article ID: {article_id}, Validation: {'Passed' if is_valid else 'Failed'}")

    # chunk_dic = get_article_points_by_chunk_name(client, collection_name)
    # for chunk_name, count in chunk_dic.items():
    #     if count > 1:
    #         print(chunk_name, count)
    #
    # article_id = "PMC_10765522"  # Replace with the desired article_id
    #
    # # Step 1: Count points
    # point_count = count_points_by_article_id(client, collection_name, article_id)
    # print(f"Number of points for article_id {article_id}: {point_count}")
    #
    # # Step 2: Delete points
    # if point_count > 0:
    #     delete_points_by_article_id(client, collection_name, article_id)
    # else:
    #     print(f"No points found for article_id {article_id}")