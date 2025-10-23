# fragment-validator/tests/conftest.py
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.update(
    {
        "S3_BUCKET": "test-bucket",
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
    }
)


@pytest.fixture
def mock_s3():
    """Mock S3 client"""
    with patch("boto3.client") as mock:
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"Contents": [{"Key": "test.csv"}]}
        s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b"col1,col2\nval1,val2")
        }
        mock.return_value = s3
        yield s3
