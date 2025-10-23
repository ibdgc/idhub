# gsid-service/main.py

import logging
import os
from contextlib import asynccontextmanager

import psycopg2
from api.routes import router
from core.config import settings
from fastapi import FastAPI
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Lifespan context manager (replaces on_startup/on_shutdown)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting GSID Service...")
    logger.info(f"Database: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
    yield
    # Shutdown
    logger.info("Shutting down GSID Service...")


app = FastAPI(
    title="GSID Service",
    description="Global Subject ID generation service for idHub",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.HOST, port=settings.PORT)
