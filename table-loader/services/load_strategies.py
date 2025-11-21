# table-loader/services/load_strategies.py

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


class LoadStrategy(ABC):
    """Abstract base class for load strategies"""

    def __init__(self, table_name: str, exclude_fields: Set[str] = None):
        self.table_name = table_name
        self.exclude_fields = exclude_fields or set()

    @abstractmethod
    def load(
        self,
        conn: psycopg2.extensions.connection,
        records: List[Dict[str, Any]],
        batch_id: str,
        source_fragment: str,
    ) -> Dict[str, Any]:
        """Load records into database"""
        pass

    def _filter_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out excluded fields"""
        return {k: v for k, v in record.items() if k not in self.exclude_fields}


class StandardLoadStrategy(LoadStrategy):
    """Standard INSERT strategy (no conflict handling)"""

    def load(
        self,
        conn: psycopg2.extensions.connection,
        records: List[Dict[str, Any]],
        batch_id: str,
        source_fragment: str,
    ) -> Dict[str, Any]:
        """Load records using standard INSERT"""
        if not records:
            return {
                "rows_attempted": 0,
                "rows_loaded": 0,
                "rows_failed": 0,
                "errors": [],
            }

        filtered_records = [self._filter_fields(r) for r in records]
        columns = list(filtered_records[0].keys())

        insert_query = f"""
            INSERT INTO {self.table_name} ({", ".join(columns)})
            VALUES %s
        """

        try:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    insert_query,
                    [tuple(r[col] for col in columns) for r in filtered_records],
                )
                rows_loaded = cursor.rowcount

            logger.info(
                f"Loaded {rows_loaded} rows into {self.table_name} (standard insert)"
            )

            return {
                "rows_attempted": len(records),
                "rows_loaded": rows_loaded,
                "rows_failed": 0,
                "errors": [],
            }

        except Exception as e:
            logger.error(f"Failed to load records: {e}")
            return {
                "rows_attempted": len(records),
                "rows_loaded": 0,
                "rows_failed": len(records),
                "errors": [str(e)],
            }


class UniversalUpsertStrategy(LoadStrategy):
    """
    Universal UPSERT strategy with change detection and audit logging

    Special handling for local_subject_ids:
    - Detects center_id changes by matching on (local_subject_id, identifier_type)
    - Deletes old records before inserting new ones with updated center_id
    - Syncs all changes to NocoDB
    """

    def __init__(
        self,
        table_name: str,
        natural_key: List[str],
        exclude_fields: Set[str] = None,
        changed_by: str = "table_loader",
    ):
        super().__init__(table_name, exclude_fields)
        self.natural_key = natural_key
        self.changed_by = changed_by

    def load(
        self,
        conn: psycopg2.extensions.connection,
        records: List[Dict[str, Any]],
        batch_id: str,
        source_fragment: str,
    ) -> Dict[str, Any]:
        """Load records with change detection and audit logging"""
        if not records:
            return {
                "rows_attempted": 0,
                "rows_loaded": 0,
                "rows_failed": 0,
                "rows_updated": 0,
                "rows_inserted": 0,
                "rows_unchanged": 0,
                "errors": [],
            }

        filtered_records = [self._filter_fields(r) for r in records]

        try:
            # Special handling for local_subject_ids table
            if self.table_name == "local_subject_ids":
                return self._load_local_subject_ids_with_center_handling(
                    conn, filtered_records, batch_id, source_fragment
                )

            # Standard upsert logic for other tables
            current_records = self._fetch_current_state(conn, filtered_records)
            current_map = {self._make_key(rec): rec for rec in current_records}

            new_records = []
            update_records = []
            unchanged_records = []

            for record in filtered_records:
                key = self._make_key(record)
                current = current_map.get(key)

                if current is None:
                    new_records.append(record)
                else:
                    changes = self._detect_changes(record, current)
                    if changes:
                        update_records.append(
                            {"record": record, "changes": changes, "natural_key": key}
                        )
                    else:
                        unchanged_records.append(record)

            rows_inserted = self._insert_new_records(conn, new_records)
            rows_updated = self._update_changed_records(
                conn, update_records, batch_id, source_fragment
            )

            logger.info(
                f"Upsert complete for {self.table_name}: "
                f"{rows_inserted} inserted, {rows_updated} updated, "
                f"{len(unchanged_records)} unchanged"
            )

            return {
                "rows_attempted": len(records),
                "rows_loaded": rows_inserted + rows_updated,
                "rows_failed": 0,
                "inserted": rows_inserted,
                "updated": rows_updated,
                "rows_unchanged": len(unchanged_records),
                "errors": [],
            }

        except Exception as e:
            logger.error(f"Upsert failed for {self.table_name}: {e}", exc_info=True)
            return {
                "rows_attempted": len(records),
                "rows_loaded": 0,
                "rows_failed": len(records),
                "inserted": 0,
                "updated": 0,
                "rows_unchanged": 0,
                "errors": [str(e)],
            }

    def _load_local_subject_ids_with_center_handling(
        self,
        conn: psycopg2.extensions.connection,
        records: List[Dict[str, Any]],
        batch_id: str,
        source_fragment: str,
    ) -> Dict[str, Any]:
        """
        Special handling for local_subject_ids with center_id changes

        Process:
        1. Find existing records by (local_subject_id, identifier_type) only
        2. If center_id differs: DELETE old + INSERT new + LOG change
        3. If center_id same: Standard upsert
        4. Sync all changes to NocoDB
        """
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        unchanged_records = []

        with conn.cursor() as cursor:
            for record in records:
                local_id = record["local_subject_id"]
                id_type = record["identifier_type"]
                new_center = record["center_id"]
                new_gsid = record["global_subject_id"]

                # Find existing record(s) by local_subject_id + identifier_type
                cursor.execute(
                    """
                    SELECT center_id, global_subject_id, created_by, created_at, updated_at
                    FROM local_subject_ids
                    WHERE local_subject_id = %s AND identifier_type = %s
                    """,
                    (local_id, id_type),
                )
                existing = cursor.fetchall()

                if not existing:
                    # New record - simple insert
                    self._insert_single_record(cursor, record)
                    rows_inserted += 1
                    self._sync_to_nocodb_single(record, "insert")

                elif len(existing) > 1:
                    # Multiple existing records - log warning
                    logger.warning(
                        f"⚠️  Found {len(existing)} existing records for "
                        f"{id_type}={local_id}. Cleaning up duplicates..."
                    )
                    # Delete all existing records
                    cursor.execute(
                        """
                        DELETE FROM local_subject_ids
                        WHERE local_subject_id = %s AND identifier_type = %s
                        """,
                        (local_id, id_type),
                    )
                    rows_deleted += cursor.rowcount

                    # Insert new record
                    self._insert_single_record(cursor, record)
                    rows_inserted += 1

                    # Log change
                    self._log_center_change(
                        cursor,
                        local_id,
                        id_type,
                        existing[0][0],  # old center_id
                        new_center,
                        batch_id,
                        source_fragment,
                    )
                    self._sync_to_nocodb_single(record, "insert")

                else:
                    # Single existing record
                    old_center, old_gsid, created_by, created_at, updated_at = existing[
                        0
                    ]

                    if old_center != new_center:
                        # Center mismatch - delete old + insert new
                        logger.info(
                            f"Center change detected: {id_type}={local_id} "
                            f"from center_id={old_center} to center_id={new_center}"
                        )

                        cursor.execute(
                            """
                            DELETE FROM local_subject_ids
                            WHERE center_id = %s AND local_subject_id = %s AND identifier_type = %s
                            """,
                            (old_center, local_id, id_type),
                        )
                        rows_deleted += cursor.rowcount

                        self._insert_single_record(cursor, record)
                        rows_inserted += 1

                        self._log_center_change(
                            cursor,
                            local_id,
                            id_type,
                            old_center,
                            new_center,
                            batch_id,
                            source_fragment,
                        )
                        self._sync_to_nocodb_single(record, "update")

                    elif old_gsid != new_gsid:
                        # GSID change - update
                        cursor.execute(
                            """
                            UPDATE local_subject_ids
                            SET global_subject_id = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE center_id = %s AND local_subject_id = %s AND identifier_type = %s
                            """,
                            (new_gsid, new_center, local_id, id_type),
                        )
                        rows_updated += cursor.rowcount

                        self._log_gsid_change(
                            cursor,
                            local_id,
                            id_type,
                            new_center,
                            old_gsid,
                            new_gsid,
                            batch_id,
                            source_fragment,
                        )
                        self._sync_to_nocodb_single(record, "update")

                    else:
                        # No changes
                        unchanged_records.append(record)

        logger.info(
            f"local_subject_ids load complete: "
            f"{rows_inserted} inserted, {rows_updated} updated, "
            f"{rows_deleted} deleted, {len(unchanged_records)} unchanged"
        )

        return {
            "rows_attempted": len(records),
            "rows_loaded": rows_inserted + rows_updated,
            "rows_failed": 0,
            "inserted": rows_inserted,
            "updated": rows_updated,
            "rows_deleted": rows_deleted,
            "rows_unchanged": len(unchanged_records),
            "errors": [],
        }

    def _insert_single_record(self, cursor, record: Dict[str, Any]):
        """Insert a single record"""
        columns = list(record.keys())
        placeholders = ", ".join(["%s"] * len(columns))

        query = f"""
            INSERT INTO {self.table_name} ({", ".join(columns)})
            VALUES ({placeholders})
        """

        cursor.execute(query, [record[col] for col in columns])

    def _log_center_change(
        self,
        cursor,
        local_id: str,
        id_type: str,
        old_center: int,
        new_center: int,
        batch_id: str,
        source_fragment: str,
    ):
        """Log center_id change to audit table"""
        import json

        record_key = {
            "local_subject_id": local_id,
            "identifier_type": id_type,
        }

        changes = {"center_id": {"old": old_center, "new": new_center}}

        cursor.execute(
            """
            INSERT INTO data_change_audit (
                table_name, record_key, changes, changed_by, 
                batch_id, source_fragment
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                self.table_name,
                json.dumps(record_key),
                json.dumps(changes),
                self.changed_by,
                batch_id,
                source_fragment,
            ),
        )

    def _log_gsid_change(
        self,
        cursor,
        local_id: str,
        id_type: str,
        center_id: int,
        old_gsid: str,
        new_gsid: str,
        batch_id: str,
        source_fragment: str,
    ):
        """Log GSID change to audit table"""
        import json

        record_key = {
            "center_id": center_id,
            "local_subject_id": local_id,
            "identifier_type": id_type,
        }

        changes = {"global_subject_id": {"old": old_gsid, "new": new_gsid}}

        cursor.execute(
            """
            INSERT INTO data_change_audit (
                table_name, record_key, changes, changed_by, 
                batch_id, source_fragment
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                self.table_name,
                json.dumps(record_key),
                json.dumps(changes),
                self.changed_by,
                batch_id,
                source_fragment,
            ),
        )

    def _sync_to_nocodb_single(self, record: Dict[str, Any], operation: str):
        """Sync single record to NocoDB"""
        try:
            # Add fragment-validator to path
            import os
            import sys

            # Get the project root (parent of table-loader)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            fragment_validator_path = os.path.join(project_root, "fragment-validator")

            if fragment_validator_path not in sys.path:
                sys.path.insert(0, fragment_validator_path)

            from clients.nocodb_client import NocoDBClient

            nocodb = NocoDBClient()

            local_id = record["local_subject_id"]
            id_type = record["identifier_type"]
            center_id = record["center_id"]

            if operation == "update":
                # Find existing record in NocoDB
                existing = nocodb.get_all_records(
                    "local_subject_ids",
                    filters=f"(local_subject_id,eq,{local_id})~and(identifier_type,eq,{id_type})",
                )

                if existing:
                    # Delete old records (there might be duplicates)
                    for old_record in existing:
                        nocodb.delete_record("local_subject_ids", old_record["Id"])

                # Insert new record
                nocodb.create_record("local_subject_ids", record)

            elif operation == "insert":
                # Check if already exists
                existing = nocodb.get_all_records(
                    "local_subject_ids",
                    filters=f"(local_subject_id,eq,{local_id})~and(identifier_type,eq,{id_type})~and(center_id,eq,{center_id})",
                )

                if not existing:
                    nocodb.create_record("local_subject_ids", record)
                else:
                    # Update existing
                    nocodb.update_record("local_subject_ids", existing[0]["Id"], record)

        except Exception as e:
            logger.warning(f"NocoDB sync failed (non-fatal): {e}")

    def _make_key(self, record: Dict[str, Any]) -> tuple:
        """Create tuple key from natural key fields"""
        return tuple(record.get(field) for field in self.natural_key)

    def _fetch_current_state(
        self, conn: psycopg2.extensions.connection, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Fetch current state of records from database"""
        if not records:
            return []

        keys = [self._make_key(r) for r in records]
        columns = list(records[0].keys())

        query = f"""
            SELECT {", ".join(columns)}
            FROM {self.table_name}
            WHERE ({", ".join(self.natural_key)}) IN %s
        """

        with conn.cursor() as cursor:
            cursor.execute(query, (tuple(keys),))
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def _detect_changes(
        self, incoming: Dict[str, Any], current: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Detect changes between incoming and current record"""
        changes = {}

        for field, new_value in incoming.items():
            if field in self.natural_key or field in self.exclude_fields:
                continue

            old_value = current.get(field)

            if old_value is None and new_value is None:
                continue

            if old_value != new_value:
                changes[field] = {"old": old_value, "new": new_value}

        return changes

    def _insert_new_records(
        self, conn: psycopg2.extensions.connection, records: List[Dict[str, Any]]
    ) -> int:
        """Insert new records"""
        if not records:
            return 0

        columns = list(records[0].keys())

        insert_query = f"""
            INSERT INTO {self.table_name} ({", ".join(columns)})
            VALUES %s
        """

        with conn.cursor() as cursor:
            execute_values(
                cursor,
                insert_query,
                [tuple(r[col] for col in columns) for r in records],
            )
            return cursor.rowcount

    def _update_changed_records(
        self,
        conn: psycopg2.extensions.connection,
        update_records: List[Dict],
        batch_id: str,
        source_fragment: str,
    ) -> int:
        """Update changed records and log to audit table"""
        if not update_records:
            return 0

        rows_updated = 0

        with conn.cursor() as cursor:
            for item in update_records:
                record = item["record"]
                changes = item["changes"]
                natural_key = item["natural_key"]

                set_clause = ", ".join(f"{field} = %s" for field in changes.keys())
                where_clause = " AND ".join(
                    f"{field} = %s" for field in self.natural_key
                )

                update_query = f"""
                    UPDATE {self.table_name}
                    SET {set_clause}
                    WHERE {where_clause}
                """

                cursor.execute(
                    update_query,
                    [changes[f]["new"] for f in changes.keys()] + list(natural_key),
                )

                if cursor.rowcount > 0:
                    rows_updated += cursor.rowcount
                    self._log_change(
                        cursor, natural_key, changes, batch_id, source_fragment
                    )

        return rows_updated

    def _log_change(
        self,
        cursor,
        natural_key: tuple,
        changes: Dict[str, Dict[str, Any]],
        batch_id: str,
        source_fragment: str,
    ):
        """Log change to data_change_audit table"""
        import json

        record_key = dict(zip(self.natural_key, natural_key))

        audit_query = """
            INSERT INTO data_change_audit (
                table_name, record_key, changes, changed_by, 
                batch_id, source_fragment
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            audit_query,
            (
                self.table_name,
                json.dumps(record_key),
                json.dumps(changes),
                self.changed_by,
                batch_id,
                source_fragment,
            ),
        )


class UpsertLoadStrategy(LoadStrategy):
    """Legacy UPSERT strategy (kept for backward compatibility)"""

    def __init__(
        self,
        table_name: str,
        conflict_columns: List[str],
        exclude_fields: Set[str] = None,
    ):
        super().__init__(table_name, exclude_fields)
        self.conflict_columns = conflict_columns

    def load(
        self,
        conn: psycopg2.extensions.connection,
        records: List[Dict[str, Any]],
        batch_id: str,
        source_fragment: str,
    ) -> Dict[str, Any]:
        """Load records using UPSERT (INSERT ... ON CONFLICT DO UPDATE)"""
        if not records:
            return {
                "rows_attempted": 0,
                "rows_loaded": 0,
                "rows_failed": 0,
                "errors": [],
            }

        filtered_records = [self._filter_fields(r) for r in records]
        columns = list(filtered_records[0].keys())
        update_columns = [c for c in columns if c not in self.conflict_columns]

        upsert_query = f"""
            INSERT INTO {self.table_name} ({", ".join(columns)})
            VALUES %s
            ON CONFLICT ({", ".join(self.conflict_columns)})
            DO UPDATE SET
                {", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)}
        """

        try:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    upsert_query,
                    [tuple(r[col] for col in columns) for r in filtered_records],
                )
                rows_loaded = cursor.rowcount

            logger.info(
                f"Upserted {rows_loaded} rows into {self.table_name} "
                f"(conflict on: {', '.join(self.conflict_columns)})"
            )

            return {
                "rows_attempted": len(records),
                "rows_loaded": rows_loaded,
                "rows_failed": 0,
                "errors": [],
            }

        except Exception as e:
            logger.error(f"Failed to upsert records: {e}")
            return {
                "rows_attempted": len(records),
                "rows_loaded": 0,
                "rows_failed": len(records),
                "errors": [str(e)],
            }
