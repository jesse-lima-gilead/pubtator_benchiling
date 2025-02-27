import os
import boto3
from botocore.exceptions import ClientError
from src.pubtator_utils.aws_handler.aws_connect import AWSConnection
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
aws_s3_config = config_loader.get_config("aws")["aws"]["s3"]

# Get the S3 client
s3_client = AWSConnection().get_client("s3")


class S3IOUtil:
    def __init__(self):
        """Initialize the S3Util class with a specific S3 bucket."""
        self.bucket_name = aws_s3_config["bucket_name"]
        self.bucket_region = aws_s3_config["bucket_region"]
        self.s3 = boto3.resource("s3", region_name=self.bucket_region)
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.client = s3_client

    def upload_file(self, file_path=None, object_name=None, content=None):
        """Upload a file or content to the S3 bucket.

        - If `content` is provided, it uploads the content directly to S3.
        - If `file_path` is provided, it uploads the file from disk.
        """
        try:
            if object_name is None:
                if file_path:
                    object_name = os.path.basename(file_path)
                else:
                    raise ValueError(
                        "Either file_path or object_name must be provided."
                    )

            if content is not None:
                # Upload content directly
                self.client.put_object(
                    Bucket=self.bucket_name, Key=file_path, Body=content
                )
                logger.info(f"Content uploaded successfully to {file_path}")
            elif file_path is not None:
                # Upload file from disk
                self.bucket.upload_file(file_path, object_name)
                logger.info(f"File {file_path} uploaded successfully.")
            else:
                raise ValueError("Either file_path or content must be provided.")

        except ClientError as e:
            logger.error(f"Failed to upload file/content: {e}")
            return False
        return True

    def download_file(self, object_name, file_name=None):
        """Download a file from S3.

        - If `file_name` is provided, save the file locally.
        - Otherwise, return the file content as a string.
        """
        try:
            obj = self.bucket.Object(object_name)
            response = obj.get()

            if file_name:
                # Save to local file
                with open(file_name, "wb") as f:
                    f.write(response["Body"].read())
                logger.info(
                    f"File {object_name} downloaded successfully to {file_name}."
                )
                return True
            else:
                # Return file content as a string
                content = response["Body"].read().decode("utf-8")
                logger.info(f"File {object_name} downloaded successfully as content.")
                return content
        except ClientError as e:
            logger.error(f"Failed to download file: {e}")
            return None

    def list_files(self, prefix=""):
        """List all files in the S3 bucket."""
        try:
            files = []
            for obj in self.bucket.objects.filter(Prefix=prefix):
                files.append(obj.key)
            logger.info(f"Files in bucket: {files}")
            return files
        except ClientError as e:
            logger.info(f"Failed to list files: {e}")
            return []

    def delete_file(self, object_name):
        """Delete a file from the S3 bucket."""
        try:
            self.bucket.Object(object_name).delete()
            logger.info(f"File {object_name} deleted successfully.")
        except ClientError as e:
            logger.info(f"Failed to delete file: {e}")
            return False
        return True

    def copy_file(self, source_bucket_name, source_key, dest_key):
        """Copy a file from another S3 bucket to this one."""
        try:
            copy_source = {"Bucket": source_bucket_name, "Key": source_key}
            self.bucket.copy(copy_source, dest_key)
            logger.info(f"File {source_key} copied to {dest_key}.")
        except ClientError as e:
            logger.info(f"Failed to copy file: {e}")
            return False
        return True

    def file_exists(self, object_name):
        """Check if a file exists in the S3 bucket."""
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=object_name)
            logger.info(f"File {object_name} exists.")
            return True
        except ClientError as e:
            logger.info(f"File {object_name} does not exist: {e}")
            return False

    def move_file(self, source_key, dest_key):
        """Move a file within the S3 bucket."""
        try:
            # Copy the file to the new location
            copy_source = {"Bucket": self.bucket_name, "Key": source_key}
            self.bucket.copy(copy_source, dest_key)
            logger.info(f"File {source_key} copied to {dest_key}.")

            # Delete the original file
            self.bucket.Object(source_key).delete()
            logger.info(f"File {source_key} deleted after moving to {dest_key}.")
        except ClientError as e:
            logger.info(f"Failed to move file: {e}")
            return False
        return True


# if __name__ == "__main__":
#     s3_util = S3IOUtil()
#
#     # Upload a file
#     s3_util.upload_file("../../data/bioc_xml/PMC-31023335.xml", "bioc_xml/PMC-31023335.xml")
#
#     # Download a file
#     s3_util.download_file("s3-key-name.txt", "path/to/local/file.txt")
#
#     # List files
#     s3_util.list_files()
#
#     # Check if a file exists
#     s3_util.file_exists("s3-key-name.txt")
#
#     # Delete a file
#     s3_util.delete_file("s3-key-name.txt")
#
#     # Copy a file from another bucket
#     s3_util.copy_file("source-bucket", "source-key.txt", "destination-key.txt")
