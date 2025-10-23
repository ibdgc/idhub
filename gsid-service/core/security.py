# gsid-service/core/security.py
import logging

from fastapi import Header, HTTPException

from .config import settings

logger = logging.getLogger(__name__)


async def verify_api_key(x_api_key: str = Header(..., alias="x-api-key")):
    """Verify API key from request header"""
    if not settings.GSID_API_KEY:
        logger.error("GSID_API_KEY not configured in environment")
        raise HTTPException(status_code=500, detail="API key not configured")

    if x_api_key != settings.GSID_API_KEY:
        logger.warning("Invalid API key attempt from client")
        raise HTTPException(status_code=403, detail="Invalid API key")

    return x_api_key
