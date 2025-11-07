# table-loader/tests/test_data_transformer_enhanced.py
from datetime import datetime

import pandas as pd
import pytest
from services.data_transformer import DataTransformer


class TestDataTransformerEnhanced:
    """Enhanced tests for DataTransformer edge cases"""

    def test_transform_with_null_values(self):
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

    def test_transform_with_unicode_characters(self):
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

    def test_transform_with_date_formats(self):
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

    def test_deduplicate_with_null_keys(self):
        """Test deduplication when key columns contain nulls"""
        transformer = DataTransformer("blood")
        df = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", None, "GSID-001"],
                "sample_id": ["SMP001", "SMP002", "SMP001"],
            }
        )

        result = transformer.deduplicate(df, ["global_subject_id", "sample_id"])
        # Should keep rows with None as they're considered unique
        assert len(result) == 2

    def test_prepare_rows_with_mixed_types(self):
        """Test prepare_rows with mixed data types"""
        transformer = DataTransformer("blood")
        df = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "count": [10, 20],
                "ratio": [0.5, 0.75],
                "date": ["2024-01-15", "2024-01-16"],
                "is_valid": [True, False],
            }
        )

        columns, values = transformer.prepare_rows(df)
        assert len(columns) == 5
        assert len(values) == 2
        assert isinstance(values[0][1], (int, float))  # count
        assert isinstance(values[0][2], float)  # ratio
        assert isinstance(values[0][4], bool)  # is_valid

    def test_transform_large_dataset_performance(self):
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

    def test_exclude_fields_case_sensitivity(self):
        """Test that field exclusion is case-sensitive"""
        exclude_fields = {"Consortium_ID"}  # Different case
        transformer = DataTransformer("blood", exclude_fields=exclude_fields)

        fragment = {
            "table": "blood",
            "records": [
                {
                    "global_subject_id": "GSID-001",
                    "consortium_id": "ID001",  # lowercase
                }
            ],
        }

        records = transformer.transform_records(fragment)
        # Should NOT exclude because case doesn't match
        assert "consortium_id" in records[0]
