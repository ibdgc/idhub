# table-loader/core/config.py
import os


class Settings:
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    S3_BUCKET = os.getenv("S3_BUCKET", "idhub-curated-fragments")

    # Database Configuration
    DB_HOST = os.getenv("DB_HOST", "idhub_db")
    DB_NAME = os.getenv("DB_NAME", "idhub")
    DB_USER = os.getenv("DB_USER", "idhub_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))

    # Load Configuration
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


settings = Settings()
