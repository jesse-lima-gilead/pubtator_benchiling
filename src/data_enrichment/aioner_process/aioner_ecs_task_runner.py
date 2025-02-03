import boto3
import os
import json
from dotenv import load_dotenv
from src.utils.config_reader import YAMLConfigLoader

# Initialize the config loader
config_loader = YAMLConfigLoader()

# aws resource config
aws_config = config_loader.get_config("aws")["aws"]

# Load environment variables
load_dotenv()

# Initialize AWS session and clients
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("SSO_REGION"),
)

s3_client = session.client("s3")
ecs_client = session.client("ecs")
ec2_client = session.client("ec2")

# Environment variables
VPC_ID = os.getenv("VPC_ID")
BUCKET_NAME = aws_config["s3"]["bucket_name"]
INPUT_DIRECTORY = os.getenv("AIONER_INPUT_DIRECTORY")
OUTPUT_DIRECTORY = os.getenv("AIONER_OUTPUT_DIRECTORY")
CLUSTER_NAME = aws_config["ecs"]["cluster_name"]
TASK_DEFINITION = aws_config["ecs"]["tasks"]["aioner"]["task_definition"]
CONTAINER_NAME = aws_config["ecs"]["tasks"]["aioner"]["container_name"]
SECURITY_GROUP_ID = os.getenv("SECURITY_GROUP_ID")


def list_subnet_ids(vpc_id):
    """Fetches the subnet IDs associated with the given VPC."""
    try:
        response = ec2_client.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        return [subnet["SubnetId"] for subnet in response["Subnets"]]
    except Exception as e:
        raise RuntimeError(f"Failed to fetch subnets for VPC {vpc_id}: {e}")


def list_files_in_s3(bucket_name, prefix):
    """Lists all files in the specified S3 bucket and prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = [
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"] != prefix
        ]
        return files
    except Exception as e:
        raise RuntimeError(f"Failed to list files in S3 bucket {bucket_name}: {e}")


def trigger_ecs_task(
    file_list, subnets, bucket_name, security_group_id, output_directory
):
    """Triggers an ECS task for processing the given file list."""
    try:
        response = ecs_client.run_task(
            cluster=CLUSTER_NAME,
            taskDefinition=TASK_DEFINITION,
            overrides={
                "containerOverrides": [
                    {
                        "name": CONTAINER_NAME,
                        "environment": [
                            {"name": "FILE_LIST", "value": json.dumps(file_list)},
                            {"name": "S3_BUCKET", "value": bucket_name},
                            {"name": "S3_OUTPUT_DIRECTORY", "value": output_directory},
                        ],
                    }
                ]
            },
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": subnets,
                    "securityGroups": [security_group_id],
                    "assignPublicIp": "ENABLED",
                }
            },
        )
        return response
    except Exception as e:
        raise RuntimeError(f"Failed to trigger ECS task: {e}")


if __name__ == "__main__":
    # Validate environment variables
    required_env_vars = [
        VPC_ID,
        BUCKET_NAME,
        CLUSTER_NAME,
        TASK_DEFINITION,
        CONTAINER_NAME,
        SECURITY_GROUP_ID,
    ]
    if not all(required_env_vars):
        raise ValueError("One or more required environment variables are missing.")

    try:
        # List all files in the input S3 directory
        files = list_files_in_s3(BUCKET_NAME, INPUT_DIRECTORY)
        if not files:
            print("No files found in the S3 bucket.")
            exit(0)
        print(f"Files to process: {files}")

        # Fetch subnet IDs for the VPC
        subnets = list_subnet_ids(VPC_ID)
        if not subnets:
            raise ValueError(f"No subnets found for VPC: {VPC_ID}")
        print(f"Subnets: {subnets}")

        # Batch files into groups of 10 for ECS task processing
        batch_size = 10
        for i in range(0, len(files), batch_size):
            file_batch = files[i : i + batch_size]
            print(f"Triggering ECS task for batch: {file_batch}")
            response = trigger_ecs_task(
                file_batch, subnets, BUCKET_NAME, SECURITY_GROUP_ID, OUTPUT_DIRECTORY
            )
            print(f"ECS Task Response: {response}")

        print(f"Successfully triggered ECS tasks for {len(files)} files.")
    except Exception as e:
        print(f"An error occurred: {e}")
