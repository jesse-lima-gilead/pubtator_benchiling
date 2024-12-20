import os

def extract_numbers_from_filenames(directory_path):
    """
    Extracts numbers from filenames in the given directory.

    Args:
        directory_path (str): Path to the directory containing files.

    Returns:
        List[str]: List of numbers extracted from filenames.
    """
    numbers = []

    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.startswith("PMC_") and file.endswith(".xml"):
                # Remove "PMC_" prefix and ".xml" suffix
                number = file.replace("PMC_", "").replace(".xml", "")
                numbers.append(number)

    return numbers


directory_path = "../../data/poc_dataset/staging/pmc_xml"  # Directory where numbers are extracted

# Step 1: Get the list of numbers from another directory
numbers_list = extract_numbers_from_filenames(directory_path)
print(numbers_list)
print(len(numbers_list))