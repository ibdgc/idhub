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

    Features:
    - Handles any table with configurable natural keys
    - Detects changes before updating
    - Logs all changes to data_change_audit table
    - Skips updates when no changes detected
    - Supports batch operations
    """

    def __init__(
        self,
        table_name: str,
        natural_key: List[str],
        exclude_fields: Set[str] = None,
        changed_by: str = "table_loader",
    ):
        """
        Initialize universal upsert strategy

        Args:
            table_name: Target table name
            natural_key: List of columns that form the natural key
            exclude_fields: Fields to exclude from upsert
            changed_by: Source identifier for audit log
        """
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
        """
        Load records with change detection and audit logging

        Process:
        1. Fetch current state from database
        2. Compare incoming vs current
        3. Insert new records
        4. Update changed records (log to audit)
        5. Skip unchanged records
        """
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
            # Step 1: Fetch current state
            current_records = self._fetch_current_state(conn, filtered_records)
            current_map = {self._make_key(rec): rec for rec in current_records}

            # Step 2: Classify records
            new_records = []
            update_records = []
            unchanged_records = []

            for record in filtered_records:
                key = self._make_key(record)
                current = current_map.get(key)

                if current is None:
                    # New record
                    new_records.append(record)
                else:
                    # Check for changes
                    changes = self._detect_changes(record, current)
                    if changes:
                        update_records.append(
                            {"record": record, "changes": changes, "natural_key": key}
                        )
                    else:
                        unchanged_records.append(record)

            # Step 3: Execute operations
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
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "rows_unchanged": len(unchanged_records),
                "errors": [],
            }

        except Exception as e:
            logger.error(f"Upsert failed for {self.table_name}: {e}", exc_info=True)
            return {
                "rows_attempted": len(records),
                "rows_loaded": 0,
                "rows_failed": len(records),
                "rows_inserted": 0,
                "rows_updated": 0,
                "rows_unchanged": 0,
                "errors": [str(e)],
            }

    def _make_key(self, record: Dict[str, Any]) -> tuple:
        """Create tuple key from natural key fields"""
        return tuple(record.get(field) for field in self.natural_key)

    def _fetch_current_state(
        self, conn: psycopg2.extensions.connection, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Fetch current state of records from database"""
        if not records:
            return []

        # Build WHERE clause for natural key matching
        # Example: WHERE (global_subject_id, sample_id) IN (('GSID-001', 'SMP001'), ...)
        keys = [self._make_key(r) for r in records]

        # Get all columns from first record (excluding excluded fields)
        columns = list(records[0].keys())

        query = f"""
            SELECT {", ".join(columns)}
            FROM {self.table_name}
            WHERE ({", ".join(self.natural_key)}) IN %s
        """

        with conn.cursor() as cursor:
            cursor.execute(query, (tuple(keys),))
            rows = cursor.fetchall()

            # Convert to list of dicts
            return [dict(zip(columns, row)) for row in rows]

    def _detect_changes(
        self, incoming: Dict[str, Any], current: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Detect changes between incoming and current record

        Returns:
            Dict of changed fields: {"field_name": {"old": value, "new": value}}
        """
        changes = {}

        for field, new_value in incoming.items():
            # Skip natural key fields (they can't change)
            if field in self.natural_key:
                continue

            # Skip excluded fields
            if field in self.exclude_fields:
                continue

            old_value = current.get(field)

            # Handle None/NULL comparison
            if old_value is None and new_value is None:
                continue

            # Detect change
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
        """
        Update changed records and log to audit table

        Args:
            update_records: List of dicts with 'record', 'changes', 'natural_key'
        """
        if not update_records:
            return 0

        rows_updated = 0

        with conn.cursor() as cursor:
            for item in update_records:
                record = item["record"]
                changes = item["changes"]
                natural_key = item["natural_key"]

                # Build UPDATE query
                set_clause = ", ".join(f"{field} = %s" for field in changes.keys())
                where_clause = " AND ".join(
                    f"{field} = %s" for field in self.natural_key
                )

                update_query = f"""
                    UPDATE {self.table_name}
                    SET {set_clause}
                    WHERE {where_clause}
                """

                # Execute update
                cursor.execute(
                    update_query,
                    [changes[f]["new"] for f in changes.keys()] + list(natural_key),
                )

                if cursor.rowcount > 0:
                    rows_updated += cursor.rowcount

                    # Log to audit table
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

        # Build record_key JSONB
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
    """
    Legacy UPSERT strategy (kept for backward compatibility)

    Use UniversalUpsertStrategy for new implementations.
    """

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
