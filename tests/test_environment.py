# tests/test_environment.py
"""
Test environment configuration
"""

import os

import pytest


class TestEnvironmentFiles:
    """Test environment configuration files"""

    def test_env_example_exists(self):
        """Test that .env.example exists"""
        assert os.path.exists(".env.example") or os.path.exists("env.example")

    def test_gitignore_exists(self):
        """Test that .gitignore exists"""
        assert os.path.exists(".gitignore")

    def test_gitignore_excludes_env(self):
        """Test that .gitignore excludes .env files"""
        with open(".gitignore") as f:
            content = f.read()

        assert ".env" in content

    def test_gitignore_excludes_secrets(self):
        """Test that .gitignore excludes sensitive files"""
        with open(".gitignore") as f:
            content = f.read()

        # Check for SSL directory which contains certs/keys
        assert "ssl/" in content or "*.pem" in content or "*.key" in content


class TestRequiredEnvironmentVariables:
    """Test required environment variables are documented"""

    def test_database_vars_documented(self):
        """Test that database variables are documented"""
        # Check if .env.example exists, otherwise skip
        if not os.path.exists(".env.example"):
            pytest.skip(".env.example not found")

        with open(".env.example") as f:
            content = f.read()

        required_vars = [
            "DB_HOST",
            "DB_NAME",
            "DB_USER",
            "DB_PASSWORD",
        ]

        for var in required_vars:
            assert var in content, f"{var} not documented in .env.example"

    def test_aws_vars_documented(self):
        """Test that AWS variables are documented"""
        if not os.path.exists(".env.example"):
            pytest.skip(".env.example not found")

        with open(".env.example") as f:
            content = f.read()

        aws_vars = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "S3_BUCKET",
        ]

        for var in aws_vars:
            assert var in content, f"{var} not documented in .env.example"
