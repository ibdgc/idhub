import pandas as pd
import pytest
from services.update_detector import UpdateDetector
from unittest.mock import Mock


class TestUpdateDetector:
    """Unit tests for UpdateDetector"""

    @pytest.fixture
    def mock_nocodb_client(self):
        """Mock NocoDB client"""
        return Mock()

    def test_all_new_records(self, mock_nocodb_client):
        """Test when all records are new (no existing data)"""
        detector = UpdateDetector(mock_nocodb_client)
        mock_nocodb_client.get_all_records.return_value = []

        incoming_data = pd.DataFrame(
            {
                "global_subject_id": ["GSID-001", "GSID-002"],
                "sample_id": ["SMP001", "SMP002"],
                "volume_ml": [5.0, 7.5],
            }
        )

        result = detector.detect_changes(
            table_name="specimen", incoming_data=incoming_data
        )

        assert result["summary"]["new"] == 2
        assert result["summary"]["updated"] == 0
        assert result["summary"]["unchanged"] == 0

    def test_detect_updates(self, mock_nocodb_client):
        """Test detection of updated records"""
        detector = UpdateDetector(mock_nocodb_client)

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
        
        mock_nocodb_client.get_all_records.return_value = current.to_dict('records')

        result = detector.detect_changes(
            table_name="specimen", incoming_data=incoming
        )

        assert result["summary"]["new"] == 0
        assert result["summary"]["updated"] == 1
        assert result["summary"]["unchanged"] == 1

        # Check the update details
        update = result["updates"][0]
        assert update['incoming']['global_subject_id'] == "GSID-001"
        
        # Find the change for 'volume_ml'
        volume_change = None
        for change in update['changes']:
            if change['field'] == 'volume_ml':
                volume_change = change
                break
        
        assert volume_change is not None, "Change for 'volume_ml' not found"
        assert volume_change['old_value'] == 5.0
        assert volume_change['new_value'] == 5.5

    def test_detect_orphaned_records(self, mock_nocodb_client):
        """Test detection of orphaned records (in DB but not in incoming)"""
        detector = UpdateDetector(mock_nocodb_client)

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
        
        mock_nocodb_client.get_all_records.return_value = current.to_dict('records')

        result = detector.detect_changes(
            table_name="specimen", incoming_data=incoming
        )

        assert result["summary"]["orphaned"] == 1
        assert result["orphaned"][0]["global_subject_id"] == "GSID-002"
