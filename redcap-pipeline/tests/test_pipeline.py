# redcap-pipeline/tests/test_pipeline.py
from unittest.mock import MagicMock, patch

import pytest
import requests

from services.pipeline import REDCapPipeline


@pytest.fixture
def mock_dependencies():
    """Mock all pipeline dependencies"""
    with patch("services.pipeline.REDCapClient") as mock_redcap, \
         patch("services.pipeline.GSIDClient") as mock_gsid, \
         patch("services.pipeline.CenterResolver") as mock_center, \
         patch("services.pipeline.DataProcessor") as mock_processor, \
         patch("services.pipeline.S3Uploader") as mock_s3:
        yield {
            "redcap": mock_redcap,
            "gsid": mock_gsid,
            "center": mock_center,
            "processor": mock_processor,
            "s3": mock_s3,
        }


@pytest.fixture
def pipeline_config():
    """Sample pipeline configuration"""
    return {
        "key": "test_project",
        "name": "Test Project",
    }


class TestREDCapPipeline:
    """Test REDCapPipeline functionality"""

    def test_pipeline_init(self, pipeline_config, mock_dependencies):
        """Test pipeline initialization"""
        pipeline = REDCapPipeline(pipeline_config)
        assert pipeline.project_key == "test_project"
        mock_dependencies["redcap"].assert_called_once_with(pipeline_config)
        mock_dependencies["gsid"].assert_called_once()
        mock_dependencies["center"].assert_called_once()
        mock_dependencies["processor"].assert_called_once_with(pipeline_config)
        mock_dependencies["s3"].assert_called_once()

    def test_pipeline_run_success(self, pipeline_config):
        """Test a successful pipeline run with a single batch"""
        # Arrange
        with patch("services.pipeline.REDCapPipeline.process_record") as mock_process_record:
            with patch("services.pipeline.REDCapClient") as mock_redcap:
                
                mock_redcap_instance = mock_redcap.return_value
                mock_redcap_instance.fetch_records_batch.side_effect = [[{"record_id": "1"}, {"record_id": "2"}], []]
                
                pipeline = REDCapPipeline(pipeline_config)
                pipeline.redcap_client = mock_redcap_instance

                # Act
                result = pipeline.run()

                # Assert
                assert mock_redcap_instance.fetch_records_batch.call_count == 2
                assert mock_process_record.call_count == 2
                assert result["status"] == "success"
                assert result["total_success"] == 2
                assert result["total_errors"] == 0

    def test_pipeline_run_multiple_batches(self, pipeline_config):
        """Test processing of multiple batches"""
        with patch("services.pipeline.REDCapPipeline.process_record") as mock_process_record:
            with patch("services.pipeline.REDCapClient") as mock_redcap:

                mock_redcap_instance = mock_redcap.return_value
                mock_redcap_instance.fetch_records_batch.side_effect = [
                    [{"record_id": "1"}],
                    [{"record_id": "2"}],
                    [],
                ]
                
                pipeline = REDCapPipeline(pipeline_config)
                pipeline.redcap_client = mock_redcap_instance

                result = pipeline.run(batch_size=1)

                assert mock_redcap_instance.fetch_records_batch.call_count == 3
                assert mock_process_record.call_count == 2
                assert result["status"] == "success"

    def test_pipeline_handles_record_processing_error(self, pipeline_config):
        """Test that an error in processing a single record does not stop the pipeline"""
        with patch("services.pipeline.REDCapPipeline.process_record") as mock_process_record:
            with patch("services.pipeline.REDCapClient") as mock_redcap:

                mock_redcap_instance = mock_redcap.return_value
                mock_redcap_instance.fetch_records_batch.side_effect = [[{"record_id": "1"}, {"record_id": "2"}], []]
                
                mock_process_record.side_effect = [Exception("Test error"), None]
                
                pipeline = REDCapPipeline(pipeline_config)
                pipeline.redcap_client = mock_redcap_instance

                result = pipeline.run()

                assert result["status"] == "success"
                assert result["total_success"] == 1
                assert result["total_errors"] == 1

    def test_pipeline_stops_on_consecutive_fetch_errors(self, pipeline_config):
        """Test that the pipeline stops after multiple consecutive fetch errors"""
        with patch("services.pipeline.REDCapClient") as mock_redcap:
            mock_redcap_instance = mock_redcap.return_value
            mock_redcap_instance.fetch_records_batch.side_effect = requests.exceptions.RequestException("API Error")
            
            pipeline = REDCapPipeline(pipeline_config)
            pipeline.redcap_client = mock_redcap_instance

            result = pipeline.run()

            assert mock_redcap_instance.fetch_records_batch.call_count == 3
            assert result["status"] == "partial_success"
            assert result["error"] == "API Error"
