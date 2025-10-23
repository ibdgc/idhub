# gsid-service/api/routes.py
import logging
from datetime import datetime

from core.security import verify_api_key
from fastapi import APIRouter, Depends, HTTPException
from services.gsid_generator import generate_unique_gsids, reserve_gsids

from .models import GSIDRequest, GSIDResponse, HealthResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(status="healthy", timestamp=datetime.utcnow().isoformat())


@router.post("/generate", response_model=GSIDResponse)
async def generate_gsids(request: GSIDRequest, api_key: str = Depends(verify_api_key)):
    """Generate and reserve GSIDs"""
    try:
        logger.info(f"Generating {request.count} GSIDs")
        gsids = generate_unique_gsids(request.count)
        reserve_gsids(gsids)
        logger.info(f"Successfully generated {len(gsids)} GSIDs")

        return GSIDResponse(gsids=gsids, count=len(gsids))

    except Exception as e:
        logger.error(f"Error generating GSIDs: {e}")
        raise HTTPException(status_code=500, detail=str(e))
