import pytest
import requests
from unittest.mock import Mock, patch
from services.nocodb_client import NocoDBClient


class TestNocoDBClient:
    """Unit tests for NocoDBClient"""

    def test_get_base_id_with_provided_id(self):
        """Test getting base ID when provided in constructor"""
        client = NocoDBClient("http://nocodb", "token", base_id="base123")

        base_id = client._get_base_id()

        assert base_id == "base123"

    def test_get_base_id_auto_detect(self):
        """Test auto-detection of base ID"""
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = {
                "list": [{"id": "auto-base-123", "title": "Test Base"}]
            }
            mock_get.return_value.raise_for_status = Mock()

            client = NocoDBClient("http://nocodb", "token")
            base_id = client._get_base_id()

            assert base_id == "auto-base-123"
            assert client._base_id_cache == "auto-base-123"

    def test_get_base_id_no_bases_raises_error(self):
        """Test error when no bases found"""
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = {"list": []}
            mock_get.return_value.raise_for_status = Mock()

            client = NocoDBClient("http://nocodb", "token")

            with pytest.raises(ValueError, match="No NocoDB bases found"):
                client._get_base_id()

    def test_get_table_id_caching(self):
        """Test that table ID is cached after first lookup"""
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.side_effect = [
                {"list": [{"id": "base123", "title": "Base"}]},
                {"list": [{"id": "table456", "table_name": "blood"}]},
            ]
            mock_get.return_value.raise_for_status = Mock()

            client = NocoDBClient("http://nocodb", "token")

            # First call
            table_id1 = client.get_table_id("blood")
            # Second call (should use cache)
            table_id2 = client.get_table_id("blood")

            assert table_id1 == "table456"
            assert table_id2 == "table456"
            # Should only call API twice (once for base, once for table)
            assert mock_get.call_count == 2

    def test_get_table_id_not_found(self):
        """Test error when table not found"""
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.side_effect = [
                {"list": [{"id": "base123", "title": "Base"}]},
                {"list": [{"id": "other-table", "table_name": "other"}]},
            ]
            mock_get.return_value.raise_for_status = Mock()

            client = NocoDBClient("http://nocodb", "token")

            with pytest.raises(ValueError, match="Table 'blood' not found"):
                client.get_table_id("blood")

    def test_get_table_metadata(self):
        """Test getting table metadata"""
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.side_effect = [
                {"list": [{"id": "base123", "title": "Base"}]},
                {"list": [{"id": "table456", "table_name": "blood"}]},
                {
                    "id": "table456",
                    "table_name": "blood",
                    "columns": [
                        {"column_name": "sample_id", "rqd": True},
                        {"column_name": "sample_type", "rqd": False},
                    ],
                },
            ]
            mock_get.return_value.raise_for_status = Mock()

            client = NocoDBClient("http://nocodb", "token")
            metadata = client.get_table_metadata("blood")

            assert metadata["table_name"] == "blood"
            assert len(metadata["columns"]) == 2

    def test_get_all_records_single_page(self):
        """Test fetching all records with single page"""
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.side_effect = [
                {"list": [{"id": "base123", "title": "Base"}]},
                {"list": [{"id": "table456", "table_name": "blood"}]},
                {
                    "list": [{"id": 1, "sample_id": "SMP1"}, {"id": 2, "sample_id": "SMP2"}],
                    "pageInfo": {"isLastPage": True},
                },
            ]
            mock_get.return_value.raise_for_status = Mock()

            client = NocoDBClient("http://nocodb", "token")
            records = client.get_all_records("blood")

            assert len(records) == 2
            assert records[0]["sample_id"] == "SMP1"

    def test_get_all_records_multiple_pages(self):
        """Test fetching all records with pagination"""
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.side_effect = [
                {"list": [{"id": "base123", "title": "Base"}]},
                {"list": [{"id": "table456", "table_name": "blood"}]},
                {
                    "list": [{"id": 1, "sample_id": "SMP1"}],
                    "pageInfo": {"isLastPage": False},
                },
                {
                    "list": [{"id": 2, "sample_id": "SMP2"}],
                    "pageInfo": {"isLastPage": True},
                },
            ]
            mock_get.return_value.raise_for_status = Mock()

            client = NocoDBClient("http://nocodb", "token")
            records = client.get_all_records("blood", limit=1)

            assert len(records) == 2

    def test_load_local_id_cache(self):
        """Test loading local_subject_ids cache"""
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.side_effect = [
                {"list": [{"id": "base123", "title": "Base"}]},
                {"list": [{"id": "table456", "table_name": "local_subject_ids"}]},
                {
                    "list": [
                        {
                            "center_id": 1,
                            "local_subject_id": "ID001",
                            "identifier_type": "consortium_id",
                            "global_subject_id": "GSID-001",
                        },
                        {
                            "center_id": 1,
                            "local_subject_id": "ID002",
                            "identifier_type": "consortium_id",
                            "global_subject_id": "GSID-002",
                        },
                    ],
                    "pageInfo": {"isLastPage": True},
                },
            ]
            mock_get.return_value.raise_for_status = Mock()

            client = NocoDBClient("http://nocodb", "token")
            cache = client.load_local_id_cache()

            assert len(cache) == 2
            assert cache[(1, "ID001", "consortium_id")] == "GSID-001"
            assert cache[(1, "ID002", "consortium_id")] == "GSID-002"

    def test_load_local_id_cache_error_handling(self, caplog):
        """Test error handling in cache loading"""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.RequestException("API Error")

            client = NocoDBClient("http://nocodb", "token")
            
            with caplog.at_level("WARNING"):
                cache = client.load_local_id_cache()

                assert "Could not load local_subject_ids cache: API Error" in caplog.text
                assert cache == {}

    def test_headers_include_token(self):
        """Test that token is included in headers"""
        client = NocoDBClient("http://nocodb", "my-secret-token")

        assert client.headers["xc-token"] == "my-secret-token"

    def test_url_trailing_slash_removed(self):
        """Test that trailing slash is removed from URL"""
        client = NocoDBClient("http://nocodb/", "token")

        assert client.url == "http://nocodb"
