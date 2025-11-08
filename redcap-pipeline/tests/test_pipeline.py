# redcap-pipeline/tests/test_pipeline.py
import json
from unittest.mock import MagicMock, Mock, patch

import pytest


class TestREDCapPipeline:
    """Test REDCapPipeline functionality"""

    @pytest.fixture
    def pipeline_config(self):
        """Sample pipeline configuration"""
        return {
            "key": "test_project",
            "name": "Test Project",
            "redcap_project_id": "123",
            "redcap_api_url": "https://test.redcap.edu/api/",
            "api_token": "test_token_12345678",
            "field_mappings": "test_field_mappings.json",
            "batch_size": 50,
        }

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all pipeline dependencies"""
        with (
            patch("services.pipeline.REDCapClient") as mock_redcap,
            patch("services.pipeline.GSIDClient") as mock_gsid,
            patch("services.pipeline.CenterResolver") as mock_center,
            patch("services.pipeline.DataProcessor") as mock_processor,
            patch("services.pipeline.S3Uploader") as mock_s3,
        ):
            yield {
                "redcap": mock_redcap,
                "gsid": mock_gsid,
                "center": mock_center,
                "processor": mock_processor,
                "s3": mock_s3,
            }

    def test_pipeline_init(self, pipeline_config, mock_dependencies):
        """Test pipeline initialization"""
        from services.pipeline import REDCapPipeline

        pipeline = REDCapPipeline(pipeline_config)

        assert pipeline.project_key == "test_project"
        assert pipeline.project_name == "Test Project"
        mock_dependencies["redcap"].assert_called_once_with(pipeline_config)
        mock_dependencies["gsid"].assert_called_once()
        mock_dependencies["center"].assert_called_once()
        mock_dependencies["processor"].assert_called_once_with(pipeline_config)
        mock_dependencies["s3"].assert_called_once()

    def test_pipeline_run_success(self):
        """Test successful pipeline run"""
        from services.pipeline import REDCapPipeline

        project_config = {
            "key": "test_project",
            "name": "Test Project",
            "redcap_project_id": "123",
            "api_token": "test_token",
            "field_mappings": "test_mappings.json",
        }

        with (
            patch("services.pipeline.REDCapClient") as mock_redcap,
            patch("services.pipeline.DataProcessor") as mock_processor,
            patch("services.pipeline.GSIDClient"),
            patch("services.pipeline.CenterResolver"),
            patch("services.pipeline.S3Uploader"),
        ):
            # Mock REDCap client
            mock_redcap_instance = MagicMock()
            mock_redcap_instance.fetch_records_batch.side_effect = [
                [{"record_id": "1"}, {"record_id": "2"}],
                [],  # Empty list signals end
            ]
            mock_redcap.return_value = mock_redcap_instance

            # Mock data processor with proper return value
            mock_processor_instance = MagicMock()
            mock_processor_instance.process_record.return_value = {
                "status": "success",
                "gsid": "GSID-TEST123",
            }
            mock_processor.return_value = mock_processor_instance

            pipeline = REDCapPipeline(project_config)
            pipeline.run(batch_size=50)

            # Verify processing was called
            assert mock_processor_instance.process_record.call_count == 2

    def test_pipeline_run_empty_batch(self, pipeline_config, mock_dependencies):
        """Test pipeline with no records"""
        from services.pipeline import REDCapPipeline

        mock_redcap_instance = mock_dependencies["redcap"].return_value
        mock_redcap_instance.fetch_records_batch.return_value = []

        pipeline = REDCapPipeline(pipeline_config)
        pipeline.run(batch_size=50)

        mock_redcap_instance.fetch_records_batch.assert_called_once()

    def test_pipeline_run_with_errors(self):
        """Test pipeline handles processing errors"""
        from services.pipeline import REDCapPipeline

        project_config = {
            "key": "test_project",
            "name": "Test Project",
            "redcap_project_id": "123",
            "api_token": "test_token",
            "field_mappings": "test_mappings.json",
        }

        with (
            patch("services.pipeline.REDCapClient") as mock_redcap,
            patch("services.pipeline.DataProcessor") as mock_processor,
            patch("services.pipeline.GSIDClient"),
            patch("services.pipeline.CenterResolver"),
            patch("services.pipeline.S3Uploader"),
        ):
            mock_redcap_instance = MagicMock()
            mock_redcap_instance.fetch_records_batch.side_effect = [
                [{"record_id": "1"}],
                [],
            ]
            mock_redcap.return_value = mock_redcap_instance

            # Mock processor to raise exception
            mock_processor_instance = MagicMock()
            mock_processor_instance.process_record.side_effect = Exception(
                "Processing error"
            )
            mock_processor.return_value = mock_processor_instance

            pipeline = REDCapPipeline(project_config)

            # Should not raise, but log error
            with pytest.raises(Exception, match="Processing error"):
                pipeline.run(batch_size=50)

    def test_pipeline_run_multiple_batches(self):
        """Test pipeline processes multiple batches"""
        from services.pipeline import REDCapPipeline

        project_config = {
            "key": "test_project",
            "name": "Test Project",
            "redcap_project_id": "123",
            "api_token": "test_token",
            "field_mappings": "test_mappings.json",
        }

        with (
            patch("services.pipeline.REDCapClient") as mock_redcap,
            patch("services.pipeline.DataProcessor") as mock_processor,
            patch("services.pipeline.GSIDClient"),
            patch("services.pipeline.CenterResolver"),
            patch("services.pipeline.S3Uploader"),
        ):
            mock_redcap_instance = MagicMock()
            mock_redcap_instance.fetch_records_batch.side_effect = [
                [{"record_id": "1"}, {"record_id": "2"}],
                [{"record_id": "3"}, {"record_id": "4"}],
                [],
            ]
            mock_redcap.return_value = mock_redcap_instance

            mock_processor_instance = MagicMock()
            mock_processor_instance.process_record.return_value = {
                "status": "success",
                "gsid": "GSID-TEST123",
            }
            mock_processor.return_value = mock_processor_instance

            pipeline = REDCapPipeline(project_config)
            pipeline.run(batch_size=2)

            # Should process 4 records across 2 batches
            assert mock_processor_instance.process_record.call_count == 4

    def test_pipeline_run_redcap_error(self, pipeline_config, mock_dependencies):
        """Test pipeline handles REDCap fetch errors"""
        import requests
        from services.pipeline import REDCapPipeline

        mock_redcap_instance = mock_dependencies["redcap"].return_value
        mock_redcap_instance.fetch_records_batch.side_effect = (
            requests.exceptions.RequestException("API Error")
        )

        pipeline = REDCapPipeline(pipeline_config)

        with pytest.raises(requests.exceptions.RequestException):
            pipeline.run(batch_size=50)
