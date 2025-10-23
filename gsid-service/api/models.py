# gsid-service/api/models.py
from typing import List

from pydantic import BaseModel, field_validator


class GSIDRequest(BaseModel):
    count: int

    @field_validator("count")
    @classmethod
    def validate_count(cls, v):
        if v < 1 or v > 1000:
            raise ValueError("Count must be between 1 and 1000")
        return v


class GSIDResponse(BaseModel):
    gsids: List[str]
    count: int


class HealthResponse(BaseModel):
    status: str
    timestamp: str
