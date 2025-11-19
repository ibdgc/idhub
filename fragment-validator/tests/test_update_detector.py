import pandas as pd
import pytest
from services.update_detector import UpdateDetector


class TestUpdateDetector:
    """Unit tests for UpdateDetector"""

    def test_all_new_records(self, mock_db_connection):
        """Test when all records are new (no existing data)"""
        detector = UpdateDetector()

        # Mock empty database
        mock_db_connection.cursor().fetchall.return_value = []

        incoming_data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP002"],
                "volume_ml": [5.0, 7.5],
            }
        )

        result = detector.analyze_changes(
            incoming_data=incoming_data,
            table_name="blood",
            natural_key=["global_subject_id", "sample_id"],
        )

        assert result["summary"]["new"] == 2
        assert result["summary"]["updated"] == 0
        assert result["summary"]["unchanged"] == 0

    def test_detect_updates(self):
        """Test detection of updated records"""
        detector = UpdateDetector()

        incoming = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP002"],
                "volume_ml": [5.5, 7.5],  # First one changed
            }
        )

        current = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP002"],
                "volume_ml": [5.0, 7.5],  # Original values
            }
        )

        result = detector._compare_dataframes(
            incoming, current, ["global_subject_id", "sample_id"]
        )

        assert result["summary"]["new"] == 0
        assert result["summary"]["updated"] == 1
        assert result["summary"]["unchanged"] == 1

        # Check the update details
        update = result["updates"][0]
        assert update["natural_key"]["global_subject_id"] == "GSID-001"
        assert update["changes"]["volume_ml"]["old"] == 5.0
        assert update["changes"]["volume_ml"]["new"] == 5.5

    def test_detect_orphaned_records(self):
        """Test detection of orphaned records (in DB but not in incoming)"""
        detector = UpdateDetector()

        incoming = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001"],
                "sample_id": ["SMP001"],
                "volume_ml": [5.0],
            }
        )

        current = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP002"],
                "volume_ml": [5.0, 7.5],
            }
        )

        result = detector._compare_dataframes(
            incoming, current, ["global_subject_id", "sample_id"]
        )

        assert result["summary"]["orphaned"] == 1
        assert result["orphaned"][0]["global_subject_id"] == "GSID-002"

    def test_format_change_summary(self):
        """Test formatting of change summary"""
        detector = UpdateDetector()

        changes = {
            "summary": {
                "total_incoming": 10,
                "new": 3,
                "updated": 2,
                "unchanged": 5,
                "orphaned": 1,
            },
            "updates": [
                {
                    "natural_key": {
                        "global_subject_id": "GSID-001",
                        "sample_id": "SMP001",
                    },
                    "changes": {"volume_ml": {"old": 5.0, "new": 5.5}},
                }
            ],
        }

        summary = detector.format_change_summary(changes)

        assert "Total incoming records: 10" in summary
        assert "New records:          3" in summary
        assert "Updated records:      2" in summary
        assert "GSID-001" in summary
        assert "volume_ml" in summary
