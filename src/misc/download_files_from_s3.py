import boto3
import os

from src.utils.aws_connect import AWSConnection
from src.utils.config_reader import YAMLConfigLoader

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
aws_s3_config = config_loader.get_config("aws")["aws"]["s3"]

def download_directory_from_s3(bucket_name, s3_directory, local_directory):
    # s3 = boto3.client('s3')
    s3 = AWSConnection().get_client("s3")
    # Ensure the local directory exists
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
    # List all objects in the specified S3 directory
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_directory):
        for obj in page.get('Contents', []):
            # Get the relative path within the directory
            file_path = obj['Key']
            # Skip directory "files" (keys that end with '/')
            if file_path.endswith('/'):
                continue

            # Set the destination path
            destination_path = os.path.join(local_directory, os.path.relpath(file_path, s3_directory))

            # Ensure any nested directories exist
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            # Download the file
            s3.download_file(bucket_name, file_path, destination_path)
            print(f"Downloaded {file_path} to {destination_path}")


if __name__ == "__main__":
    # Example usage
    bucket_name = aws_s3_config["bucket_name"]
    s3_directory = 'gnorm2_annotated/bioformer_annotated'  # S3 directory path
    local_directory = '../../data/poc_dataset/ner_processed/gnorm2_annotated'  # Local path to save files
    download_directory_from_s3(bucket_name, s3_directory, local_directory)
