import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # LabKey Configuration
    LABKEY_HOST: str = os.getenv("LABKEY_HOST", "ibdgc.app.labkey.host")
    LABKEY_PROJECT: str = os.getenv("LABKEY_PROJECT", "IBDGC")
    LABKEY_SCHEMA: str = os.getenv("LABKEY_SCHEMA", "samples")
    LABKEY_API_KEY: str = os.getenv("LABKEY_API_KEY", "")

    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "idhub")
    DB_USER: str = os.getenv("DB_USER", "idhub_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    # Sync Configuration
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
    DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"

    class Config:
        env_file = ".env"


settings = Settings()
