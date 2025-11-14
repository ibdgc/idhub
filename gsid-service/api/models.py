# gsid-service/api/models.py
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class IdentifierInfo(BaseModel):
    """Single identifier for a subject"""

    local_subject_id: str
    identifier_type: str = "primary"


class MultiIdentifierSubjectRequest(BaseModel):
    """Request to register a subject with multiple identifiers"""

    center_id: int
    identifiers: List[IdentifierInfo]
    registration_year: Optional[date] = None
    control: bool = False
    created_by: str = "system"

    @field_validator("registration_year", mode="before")
    @classmethod
    def validate_year(cls, v):
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            if len(v) >= 10 and "-" in v:
                try:
                    return date.fromisoformat(v[:10])
                except ValueError:
                    pass
            try:
                year = int(v)
                if 1900 <= year <= 2100:
                    return date(year, 1, 1)
            except ValueError:
                pass
        raise ValueError(f"Invalid registration_year format: {v}")


class MultiIdentifierSubjectResponse(BaseModel):
    """Response for multi-identifier registration"""

    gsid: str
    center_id: int
    action: str
    identifiers_processed: List[dict]
    conflicts: Optional[List[str]] = None
    match_strategy: Optional[str] = None
    confidence: Optional[float] = None
    message: Optional[str] = None


class CandidateID(BaseModel):
    """Single candidate identifier"""

    local_subject_id: str = Field(..., min_length=1, max_length=255)
    identifier_type: str = Field(default="primary", max_length=50)

    @field_validator("local_subject_id")
    @classmethod
    def validate_local_id(cls, v):
        if not v or not v.strip():
            raise ValueError("local_subject_id cannot be empty")
        return v.strip()

    @field_validator("identifier_type")
    @classmethod
    def validate_identifier_type(cls, v):
        if not v or not v.strip():
            raise ValueError("identifier_type cannot be empty")
        return v.strip()


class SubjectRequest(BaseModel):
    """Single subject registration request"""

    center_id: int = Field(..., ge=0)
    local_subject_id: str = Field(..., min_length=1, max_length=255)
    identifier_type: str = Field(default="primary", max_length=50)
    registration_year: Optional[date] = None
    control: bool = False
    created_by: str = Field(default="api", max_length=100)

    @field_validator("local_subject_id")
    @classmethod
    def validate_local_id(cls, v):
        if not v or not v.strip():
            raise ValueError("local_subject_id cannot be empty")
        return v.strip()

    @field_validator("registration_year", mode="before")
    @classmethod
    def parse_registration_year(cls, v):
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            from datetime import datetime

            try:
                return datetime.strptime(v, "%Y-%m-%d").date()
            except ValueError:
                try:
                    return datetime.strptime(v, "%Y").date()
                except ValueError:
                    raise ValueError(
                        "registration_year must be in YYYY-MM-DD or YYYY format"
                    )
        return v


class MultiCandidateSubjectRequest(BaseModel):
    """Multi-candidate subject registration request"""

    center_id: int = Field(..., ge=0)
    candidate_ids: List[CandidateID] = Field(..., min_length=1)
    registration_year: Optional[date] = None
    control: bool = False
    created_by: str = Field(default="api", max_length=100)

    @field_validator("candidate_ids")
    @classmethod
    def validate_candidate_ids(cls, v):
        if not v or len(v) == 0:
            raise ValueError("At least one candidate ID is required")

        # Check for duplicate identifier_types
        types_seen = set()
        for candidate in v:
            if candidate.identifier_type in types_seen:
                raise ValueError(
                    f"Duplicate identifier_type: {candidate.identifier_type}"
                )
            types_seen.add(candidate.identifier_type)

        return v

    @field_validator("registration_year", mode="before")
    @classmethod
    def parse_registration_year(cls, v):
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            from datetime import datetime

            try:
                return datetime.strptime(v, "%Y-%m-%d").date()
            except ValueError:
                try:
                    return datetime.strptime(v, "%Y").date()
                except ValueError:
                    raise ValueError(
                        "registration_year must be in YYYY-MM-DD or YYYY format"
                    )
        return v


class BatchSubjectRequest(BaseModel):
    """Batch registration request"""

    requests: List[SubjectRequest] = Field(..., min_length=1, max_length=1000)


class BatchMultiCandidateRequest(BaseModel):
    """Batch multi-candidate registration request"""

    requests: List[MultiCandidateSubjectRequest] = Field(
        ..., min_length=1, max_length=1000
    )


class SubjectResponse(BaseModel):
    """Subject registration response"""

    gsid: Optional[str] = None
    local_subject_id: str
    identifier_type: str
    center_id: int
    action: str
    match_strategy: Optional[str] = None
    confidence: Optional[float] = None
    message: Optional[str] = None
    review_reason: Optional[str] = None
    matched_gsids: Optional[List[str]] = None
    validation_warnings: Optional[List[str]] = None
    previous_center_id: Optional[int] = None
    new_center_id: Optional[int] = None


class MultiCandidateResponse(BaseModel):
    """Multi-candidate registration response"""

    gsid: Optional[str] = None
    candidate_ids: List[CandidateID]
    center_id: int
    action: str
    match_strategy: Optional[str] = None
    confidence: Optional[float] = None
    message: Optional[str] = None
    review_reason: Optional[str] = None
    matched_gsids: Optional[List[str]] = None
    validation_warnings: Optional[List[str]] = None
    previous_center_id: Optional[int] = None
    new_center_id: Optional[int] = None


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    service: str
    version: str


__all__ = [
    "IdentifierInfo",
    "MultiIdentifierSubjectRequest",
    "MultiIdentifierSubjectResponse",
    "CandidateID",
    "SubjectRequest",
    "MultiCandidateSubjectRequest",
    "BatchSubjectRequest",
    "BatchMultiCandidateRequest",
    "SubjectResponse",
    "MultiCandidateResponse",
    "HealthResponse",
]
