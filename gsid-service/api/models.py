# gsid-service/api/models.py
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class SubjectRequest(BaseModel):
    center_id: int
    local_subject_id: str
    identifier_type: str = "primary"
    registration_year: Optional[date] = None
    control: bool = False
    created_by: str = "system"

    @field_validator("registration_year", mode="before")
    @classmethod
    def validate_year(cls, v):
        if v is None:
            return None

        # If already a date object, return it
        if isinstance(v, date):
            return v

        # If it's a string, try to parse it
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None

            # Handle YYYY-MM-DD format
            if len(v) >= 10 and "-" in v:
                try:
                    return date.fromisoformat(v[:10])
                except ValueError:
                    pass

            # Handle YYYY format - convert to January 1st of that year
            if len(v) == 4 and v.isdigit():
                year = int(v)
                if 1900 <= year <= 2100:
                    return date(year, 1, 1)

        # If it's an integer year, convert to January 1st
        if isinstance(v, int):
            if 1900 <= v <= 2100:
                return date(v, 1, 1)

        return None


class ResolutionResponse(BaseModel):
    gsid: str
    action: str
    match_strategy: str
    confidence: float
    requires_review: bool
    review_reason: Optional[str] = None


class BatchSubjectRequest(BaseModel):
    requests: List[SubjectRequest]


class HealthResponse(BaseModel):
    status: str
    database: str


class UpdateCenterRequest(BaseModel):
    center_id: int = Field(..., gt=0, description="New center ID for the subject")
