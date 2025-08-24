from os import getenv

import boto3
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

class Config(BaseSettings):
    """Configuration settings for the FastAPI service."""

    # Service settings defaults
    service_name: str = "FastAPI-service"
    build_version: str = "1.0.0"

    MODEL: str = getenv("MODEL", default="")
    DIAS_PREDECIDOS: int = getenv("DIAS_PREDECIDOS", default=30)
    AWS_BUCKET_NAME: str = getenv("AWS_BUCKET_NAME", default="")
    AWS_OBJECT_KEY: str = getenv("AWS_OBJECT_KEY", default="")
    AWS_ENDPOINT_URL_S3: str = getenv("AWS_ENDPOINT_URL_S3", default="")
    AWS_ACCESS_KEY_ID: str = getenv("AWS_ACCESS_KEY_ID", default="")
    AWS_SECRET_ACCESS_KEY: str = getenv("AWS_SECRET_ACCESS_KEY", default="")

settings = Config()
s3 = boto3.client(
    "s3",
    endpoint_url=settings.AWS_ENDPOINT_URL_S3,
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
)
