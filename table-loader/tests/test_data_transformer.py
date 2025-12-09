# table-loader/tests/test_data_transformer.py
from datetime import date, datetime

import pandas as pd
import pytest
from services.data_transformer import DataTransformer
from unittest.mock import patch


class TestDataTransformer:
    """Tests for DataTransformer"""

    @pytest.fixture
    def mock_db_manager(self):
        with patch("services.data_transformer.db_manager") as mock_manager:
            mock_manager.get_table_schema.return_value = {
                "global_subject_id": "text",
                "sample_id": "text",
                "notes": "text",
                "date_collected": "date",
                "count": "integer",
                "ratio": "numeric",
                "is_valid": "boolean",
            }
            yield mock_manager

    def test_transform_with_null_values(self, mock_db_manager):
        """Test handling of null/None values"""
        transformer = DataTransformer("blood")
        fragment = {
            "table": "blood",
            "records": [
                {"global_subject_id": "GSID-001", "sample_id": "SMP001", "notes": None},
                {"global_subject_id": "GSID-002", "sample_id": None, "notes": "test"},
            ],
        }

        records = transformer.transform_records(fragment)
        assert len(records) == 2
        assert records[0]["notes"] is None
        assert records[1]["sample_id"] is None

    def test_transform_with_unicode_characters(self, mock_db_manager):
        """Test handling of unicode and special characters"""
        transformer = DataTransformer("blood")
        fragment = {
            "table": "blood",
            "records": [
                {"global_subject_id": "GSID-001", "notes": "Test™ 中文 émojis 🎉"},
            ],
        }

        records = transformer.transform_records(fragment)
        assert records[0]["notes"] == "Test™ 中文 émojis 🎉"

    def test_transform_with_date_formats(self, mock_db_manager):
        """Test handling of various date formats"""
        transformer = DataTransformer("blood")
        fragment = {
            "table": "blood",
            "records": [
                {"global_subject_id": "GSID-001", "date_collected": "2024-01-15"},
                {
                    "global_subject_id": "GSID-002",
                    "date_collected": "2024-01-15T10:30:00",
                },
            ],
        }

        records = transformer.transform_records(fragment)
        assert len(records) == 2
        assert records[0]["date_collected"] == date(2024, 1, 15)
        # This will be None because the format is wrong for a 'date' type
        assert records[1]["date_collected"] is None

    def test_transform_large_dataset_performance(self, mock_db_manager):
        """Test performance with large dataset"""
        transformer = DataTransformer("blood")

        # Create 10,000 records
        records = [
            {
                "global_subject_id": f"GSID-{i:06d}",
                "sample_id": f"SMP{i:06d}",
                "sample_type": "Blood",
            }
            for i in range(10000)
        ]

        fragment = {"table": "blood", "records": records}

        import time

        start = time.time()
        result = transformer.transform_records(fragment)
        duration = time.time() - start

        assert len(result) == 10000
        assert duration < 5.0  # Should complete in under 5 seconds

    def test_exclude_fields_case_insensitivity(self, mock_db_manager):
        """Test that field exclusion is case-insensitive"""
        # System columns are always excluded
        transformer = DataTransformer("blood")

        fragment = {
            "table": "blood",
            "records": [
                {
                    "global_subject_id": "GSID-001",
                    "Id": 123,  # Should be excluded
                }
            ],
        }

        records = transformer.transform_records(fragment)
        assert "Id" not in records[0]

    def test_empty_dataframe(self, mock_db_manager):
        """Test with an empty dataframe"""
        transformer = DataTransformer("blood")
        df = pd.DataFrame()
        records = transformer.transform_records(df)
        assert len(records) == 0

    def test_record_with_invalid_gsid_is_skipped(self, mock_db_manager):
        """Test that records with invalid GSIDs are skipped"""
        transformer = DataTransformer("blood")
        fragment = {
            "table": "blood",
            "records": [
                {"global_subject_id": "GSID-001", "sample_id": "SMP001"},
                {"global_subject_id": None, "sample_id": "SMP002"},
                {"global_subject_id": "nan", "sample_id": "SMP003"},
            ],
        }

        records = transformer.transform_records(fragment)
        assert len(records) == 1
        assert records[0]["global_subject_id"] == "GSID-001"
