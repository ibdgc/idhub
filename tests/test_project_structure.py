# tests/test_project_structure.py
"""
Test project structure and organization
"""

import glob
import os

import pytest


class TestProjectStructure:
    """Test overall project structure"""

    def test_service_directories_exist(self):
        """Test that all service directories exist"""
        services = [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ]

        for service in services:
            assert os.path.isdir(service), f"{service} directory not found"

    def test_database_directory_exists(self):
        """Test that database directory exists"""
        assert os.path.isdir("database")

    def test_nginx_directory_exists(self):
        """Test that nginx directory exists"""
        assert os.path.isdir("nginx")

    def test_readme_exists(self):
        """Test that README.md exists"""
        assert os.path.exists("README.md")


class TestServiceStructure:
    """Test that each service has required structure"""

    @pytest.mark.parametrize(
        "service",
        [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ],
    )
    def test_service_has_dockerfile(self, service):
        """Test that service has Dockerfile"""
        dockerfile_path = os.path.join(service, "Dockerfile")
        assert os.path.exists(dockerfile_path), f"{service}/Dockerfile not found"

    @pytest.mark.parametrize(
        "service",
        [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ],
    )
    def test_service_has_test_dockerfile(self, service):
        """Test that service has Dockerfile.test"""
        dockerfile_path = os.path.join(service, "Dockerfile.test")
        assert os.path.exists(dockerfile_path), f"{service}/Dockerfile.test not found"

    @pytest.mark.parametrize(
        "service",
        [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ],
    )
    def test_service_has_requirements(self, service):
        """Test that service has requirements.txt"""
        req_path = os.path.join(service, "requirements.txt")
        assert os.path.exists(req_path), f"{service}/requirements.txt not found"

    @pytest.mark.parametrize(
        "service",
        [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ],
    )
    def test_service_has_test_requirements(self, service):
        """Test that service has requirements-test.txt"""
        req_path = os.path.join(service, "requirements-test.txt")
        assert os.path.exists(req_path), f"{service}/requirements-test.txt not found"

    @pytest.mark.parametrize(
        "service",
        [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ],
    )
    def test_service_has_tests_directory(self, service):
        """Test that service has tests directory"""
        tests_path = os.path.join(service, "tests")
        assert os.path.isdir(tests_path), f"{service}/tests directory not found"

    @pytest.mark.parametrize(
        "service",
        [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ],
    )
    def test_service_has_pytest_ini(self, service):
        """Test that service has pytest.ini"""
        pytest_path = os.path.join(service, "pytest.ini")
        assert os.path.exists(pytest_path), f"{service}/pytest.ini not found"

    @pytest.mark.parametrize(
        "service",
        [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ],
    )
    def test_service_has_coveragerc(self, service):
        """Test that service has .coveragerc"""
        coverage_path = os.path.join(service, ".coveragerc")
        assert os.path.exists(coverage_path), f"{service}/.coveragerc not found"


class TestDatabaseStructure:
    """Test database directory structure"""

    def test_database_directory_has_content(self):
        """Test that database directory has SQL files"""
        # Check for any SQL files in database directory
        sql_files = glob.glob("database/**/*.sql", recursive=True)
        assert len(sql_files) > 0, "No SQL files found in database directory"

    def test_database_has_schema_or_init(self):
        """Test that database has schema or init file"""
        # Check for common database file names
        possible_files = [
            "database/init.sql",
            "database/schema.sql",
            "database/001_init.sql",
            "database/init-scripts/01-schema.sql",
        ]

        # Also check for any SQL file in database directory or subdirectories
        sql_files = glob.glob("database/**/*.sql", recursive=True)

        # Accept if any of the specific files exist OR if there are any SQL files
        exists = any(os.path.exists(f) for f in possible_files) or len(sql_files) > 0

        assert exists, f"Database directory should have SQL files. Found: {sql_files}"
