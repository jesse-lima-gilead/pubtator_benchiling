import os

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv

from src.utils.config_handler.config_reader import YAMLConfigLoader
from src.utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
aws_config = config_loader.get_config("aws")["aws"]["infra"]


class AWSConnection:
    def __init__(self):
        self.config = aws_config
        self.session = None
        self.client = None
        load_dotenv()  # Load environment variables from .env file

    def setup_session(self):
        """Sets up and maintains the AWS session and client."""
        aws_sso_region = self.config.get("aws", {}).get(
            "region", os.getenv("SSO_REGION")
        )
        try:
            self.session = boto3.Session(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
                region_name=aws_sso_region,
            )
            # Test the connection by creating an STS client and calling 'get_caller_identity'
            self.client = self.session.client("sts")
            if self.test_connection():
                return self.session
            else:
                return None

        except KeyError as e:
            raise Exception(f"Missing AWS credentials in the environment: {str(e)}")
        except NoCredentialsError:
            raise Exception("Credentials are not available.")
        except PartialCredentialsError:
            raise Exception("Incomplete credentials provided.")

    def test_connection(self) -> bool:
        """Tests if the AWS connection is successful."""
        try:
            # Call a basic AWS API to verify credentials (STS get_caller_identity)
            identity = self.client.get_caller_identity()
            logger.info(f"Successfully connected to AWS as: {identity['Arn']}")
            return True
        except Exception as e:
            logger.info(f"Failed to connect to AWS: {str(e)}")
            return False

    def get_client(self, service_name: str):
        """Returns a client for a specific AWS service."""
        aws_session = self.setup_session()
        if not aws_session:
            raise Exception("AWS session is not initialized.")
        return aws_session.client(service_name)


# # Usage Example:
# if __name__ == "__main__":
#     aws_connection = AWSConnection()
#     connection_status = aws_connection.setup_connection()
#
#     if connection_status:
#         logger.info("AWS connection established and maintained.")
#     else:
#         logger.info("Failed to establish AWS connection.")
