# gsid-service/api/models.py
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class IdentifierInput(BaseModel):
    """Single identifier for a subject"""

    local_subject_id: str = Field(..., min_length=1, max_length=255)
    identifier_type: str = Field(default="primary", max_length=50)

    @field_validator("local_subject_id")
    @classmethod
    def validate_local_id(cls, v):
        if not v or not v.strip():
            raise ValueError("local_subject_id cannot be empty")
        return v.strip()


class SubjectRegistrationRequest(BaseModel):
    """Register one subject with one or more identifiers"""

    center_id: int = Field(..., ge=0)
    identifiers: List[IdentifierInput] = Field(..., min_length=1)
    registration_year: Optional[date] = None
    control: bool = False
    created_by: str = Field(default="system", max_length=100)

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


class SubjectRegistrationResponse(BaseModel):
    """Response for subject registration"""

    gsid: str
    action: str  # "create_new", "link_existing", "conflict_resolved"
    identifiers_linked: int
    conflicts: Optional[List[str]] = None
    conflict_resolution: Optional[str] = None
    message: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    database: str = "connected"
