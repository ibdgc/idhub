# gsid-service/main.py

import logging

from api.routes import router
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="GSID Service",
    description="Global Subject ID generation service for idHub",
    version="1.0.0",
)

app.include_router(router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)