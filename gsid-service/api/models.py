# gsid-service/api/models.py
from typing import List, Optional

from pydantic import BaseModel, field_validator


class SubjectRequest(BaseModel):
    center_id: int
    local_subject_id: str
    identifier_type: str = "primary"
    registration_year: Optional[int] = None
    control: bool = False
    created_by: str = "system"

    @field_validator("registration_year")
    @classmethod
    def validate_year(cls, v):
        if v is None:
            return None
        if isinstance(v, int):
            if 1900 <= v <= 2100:
                return v
            return None
        if "-" in str(v):
            v = str(v).split("-")[0]
        if len(str(v)) == 4 and str(v).isdigit():
            year = int(v)
            if 1900 <= year <= 2100:
                return year
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