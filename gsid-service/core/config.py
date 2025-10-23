# gsid-service/core/config.py
import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # API Configuration
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    GSID_API_KEY: str = os.getenv("GSID_API_KEY", "")

    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "idhub_db")
    DB_NAME: str = os.getenv("DB_NAME", "idhub")
    DB_USER: str = os.getenv("DB_USER", "idhub_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))

    class Config:
        case_sensitive = True


settings = Settings()
