# src/db.py
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import boto3
import json
import urllib.parse
import os

def get_db_url(
    config_name: str = "aws",
    region: str = "us-west-2",
):
    config_loader = YAMLConfigLoader()
    paths_config = config_loader.get_config(config_name)
    hpc_env = os.getenv("HPC_ENV")
    rds_config = paths_config[config_name]["RDS"][hpc_env.upper()]
    username = rds_config["db_username"]
    host = rds_config["db_host"]
    dbname = rds_config["db_name"]
    port = rds_config.get("db_port", 5432)
    secret_arn = rds_config["db_secret_arn"]

    client = boto3.client("secretsmanager", region_name=region)

    response = client.get_secret_value(SecretId=secret_arn)

    # Secret can be string or JSON
    secret = response.get("SecretString")
    if not secret:
        raise RuntimeError("SecretString not found in secret")

    try:
        secret_dict = json.loads(secret)
        password = secret_dict.get("password", secret)
    except json.JSONDecodeError:
        password = secret

    # URL-encode password
    password = urllib.parse.quote_plus(password)

    db_url = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    return db_url

db_url = get_db_url()

engine = create_engine(db_url)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a Session instance
# session = Session()

