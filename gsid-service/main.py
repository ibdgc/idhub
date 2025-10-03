# gsid-service/main.py
import os
from contextlib import contextmanager
from typing import Optional

import psycopg2
from fastapi import FastAPI, HTTPException
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, validator

app = FastAPI(title="idHub GSID Registration Service", version="1.0.0")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "idhub_db"),
    "database": os.getenv("DB_NAME", "idhub"),
    "user": os.getenv("DB_USER", "idhub_user"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432)),
}


@contextmanager
def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class SubjectRegistration(BaseModel):
    center_id: int
    local_subject_id: str
    identifier_type: Optional[str] = "primary"
    registration_year: Optional[str] = None
    control: Optional[bool] = False
    withdrawn: Optional[bool] = False
    family_id: Optional[str] = None

    @validator("local_subject_id")
    def validate_local_id(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError("local_subject_id cannot be empty")
        return v.strip()


class GSIDResponse(BaseModel):
    global_subject_id: int
    center_id: int
    local_subject_id: str
    message: str


@app.post("/register", response_model=GSIDResponse)
def register_subject(registration: SubjectRegistration):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            "SELECT center_id FROM centers WHERE center_id = %s",
            (registration.center_id,),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Center not found")

        cur.execute(
            "SELECT global_subject_id FROM local_subject_ids WHERE center_id = %s AND local_subject_id = %s",
            (registration.center_id, registration.local_subject_id),
        )
        existing = cur.fetchone()
        if existing:
            return GSIDResponse(
                global_subject_id=existing["global_subject_id"],
                center_id=registration.center_id,
                local_subject_id=registration.local_subject_id,
                message="Subject already registered",
            )

        if registration.family_id:
            cur.execute(
                "SELECT family_id FROM family WHERE family_id = %s",
                (registration.family_id,),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Family not found")

        cur.execute(
            """INSERT INTO subjects (center_id, registration_year, control, withdrawn, family_id)
               VALUES (%s, %s, %s, %s, %s) RETURNING global_subject_id""",
            (
                registration.center_id,
                registration.registration_year,
                registration.control,
                registration.withdrawn,
                registration.family_id,
            ),
        )
        gsid = cur.fetchone()["global_subject_id"]

        cur.execute(
            """INSERT INTO local_subject_ids (center_id, local_subject_id, identifier_type, global_subject_id)
               VALUES (%s, %s, %s, %s)""",
            (
                registration.center_id,
                registration.local_subject_id,
                registration.identifier_type,
                gsid,
            ),
        )

        return GSIDResponse(
            global_subject_id=gsid,
            center_id=registration.center_id,
            local_subject_id=registration.local_subject_id,
            message="Subject registered successfully",
        )


@app.get("/lookup/{center_id}/{local_subject_id}")
def lookup_subject(center_id: int, local_subject_id: str):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT l.global_subject_id, l.center_id, l.local_subject_id, l.identifier_type,
                      s.registration_year, s.control, s.withdrawn, s.family_id
               FROM local_subject_ids l
               JOIN subjects s ON l.global_subject_id = s.global_subject_id
               WHERE l.center_id = %s AND l.local_subject_id = %s""",
            (center_id, local_subject_id),
        )
        result = cur.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Subject not found")
        return result


@app.get("/health")
def health_check():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
