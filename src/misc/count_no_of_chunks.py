import os
import json

def count_total_list_length(directory_path: str) -> int:
    """
    Count the total length of all lists in JSON files within a directory.

    Args:
        directory_path (str): Path to the directory containing JSON files.

    Returns:
        int: The total length of all lists across all JSON files.
    """
    total_length = 0

    # Iterate over files in the directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        # Process only JSON files
        if filename.endswith(".json") and os.path.isfile(file_path):
            with open(file_path, 'r') as file:
                try:
                    data = json.load(file)

                    # Ensure the file contains a list
                    if isinstance(data, list):
                        total_length += len(data)
                    else:
                        print(f"File {filename} does not contain a list; skipping.")

                except json.JSONDecodeError:
                    print(f"Error decoding JSON for file: {filename}")

    return total_length


if __name__ == "__main__":
    # Directory containing JSON files
    directory_path = "../../data/enhanced_golden_dataset/indexing/chunks"  # Replace with your actual directory path

    # Get total list length
    total_length = count_total_list_length(directory_path)

    print(f"Total length of all lists in JSON files: {total_length}")
