import boto3
import os
from transformers import AutoModel, AutoTokenizer
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def load_transformer_from_s3_path(
    s3_bucket, s3_model_path, local_dir, model_class=None, tokenizer_class=None
):
    s3 = boto3.client("s3")
    os.makedirs(local_dir, exist_ok=True)

    try:
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_model_path)

        if "Contents" in response:
            for obj in response["Contents"]:
                s3_key = obj["Key"]
                local_path = os.path.join(
                    local_dir, os.path.relpath(s3_key, s3_model_path)
                )
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                s3.download_file(s3_bucket, s3_key, local_path)

            if model_class and tokenizer_class:
                model = model_class.from_pretrained(local_dir)
                tokenizer = tokenizer_class.from_pretrained(local_dir)
                logger.info(
                    f"Model and tokenizer loaded from {s3_bucket}/{s3_model_path}"
                )
            elif model_class:
                logger.info(f"Model loaded from {s3_bucket}/{s3_model_path}")
            else:
                logger.info("Please provide a model class to load")
        else:
            logger.info(f"No model found at s3://{s3_bucket}/{s3_model_path}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    # Example usage
    model_type = "embeddings_model"
    model_name = "pubmedbert"
    model_path_type = config_loader.get_config("paths")["model"]["type"]
    model_path_config = config_loader.get_config("paths")["model"][model_path_type][
        model_name
    ]
    s3_bucket = model_path_config["s3"]["bucket"]
    s3_bucket_model_path = model_path_config["s3"]["model_path"]
    model_local_path = model_path_config["local"]["model_path"]

    model_class = AutoModel
    tokenizer_class = AutoTokenizer

    # Load the model and tokenizer from S3
    load_transformer_from_s3_path(
        s3_bucket=s3_bucket,
        s3_bucket_model_path=s3_bucket_model_path,
        local_dir=model_local_path,
        model_class=model_class,
        tokenizer_class=tokenizer_class,
    )
