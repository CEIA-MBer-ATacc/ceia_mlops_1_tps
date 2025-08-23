from dotenv import load_dotenv
from os import getenv
from pydantic_settings import BaseSettings

load_dotenv()

class Config(BaseSettings):
    """Configuration settings for the FastAPI service."""

    # Service settings defaults
    service_name: str = "FastAPI-service"
    build_version: str = "1.0.0"

    MODEL: str = getenv("MODEL", default="")
    DIAS_PREDECIDOS: int = getenv("DIAS_PREDECIDOS", default=30)
    
settings = Config()