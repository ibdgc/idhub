# tests/test_docker_services.py
"""
Root-level integration tests for Docker services
"""

import pytest


class TestDockerConfiguration:
    """Test Docker configuration files"""

    def test_docker_compose_exists(self):
        """Test that docker-compose.yml exists"""
        import os

        assert os.path.exists("docker-compose.yml")

    def test_docker_compose_test_exists(self):
        """Test that docker-compose.test.yml exists"""
        import os

        assert os.path.exists("docker-compose.test.yml")

    def test_docker_compose_valid_yaml(self):
        """Test that docker-compose.yml is valid YAML"""
        import yaml

        with open("docker-compose.yml") as f:
            config = yaml.safe_load(f)

        assert "services" in config
        assert "networks" in config

    def test_docker_compose_test_valid_yaml(self):
        """Test that docker-compose.test.yml is valid YAML"""
        import yaml

        with open("docker-compose.test.yml") as f:
            config = yaml.safe_load(f)

        assert "services" in config


class TestServiceDefinitions:
    """Test service definitions in docker-compose"""

    def test_required_services_defined(self):
        """Test that all required services are defined"""
        import yaml

        with open("docker-compose.yml") as f:
            config = yaml.safe_load(f)

        services = config.get("services", {})

        # Check for core services
        assert "idhub_db" in services
        assert "gsid-service" in services
        assert "redcap-pipeline" in services
        assert "fragment-validator" in services
        assert "table-loader" in services

    def test_test_services_defined(self):
        """Test that test services are defined"""
        import yaml

        with open("docker-compose.test.yml") as f:
            config = yaml.safe_load(f)

        services = config.get("services", {})

        # Use actual service names from docker-compose.test.yml
        assert "test-gsid" in services
        assert "test-redcap" in services
        assert "test-validator" in services  # Not test-fragment
        assert "test-loader" in services  # Not test-table

    def test_database_service_configuration(self):
        """Test database service has required configuration"""
        import yaml

        with open("docker-compose.yml") as f:
            config = yaml.safe_load(f)

        db_service = config["services"]["idhub_db"]

        assert "image" in db_service
        assert "postgres" in db_service["image"]
        assert "environment" in db_service
        assert "volumes" in db_service

    def test_gsid_service_configuration(self):
        """Test GSID service has required configuration"""
        import yaml

        with open("docker-compose.yml") as f:
            config = yaml.safe_load(f)

        gsid_service = config["services"]["gsid-service"]

        assert "build" in gsid_service
        assert "environment" in gsid_service
        assert "depends_on" in gsid_service


class TestNetworkConfiguration:
    """Test network configuration"""

    def test_network_defined(self):
        """Test that idhub_network is defined"""
        import yaml

        with open("docker-compose.yml") as f:
            config = yaml.safe_load(f)

        networks = config.get("networks", {})
        assert "idhub_network" in networks

    def test_services_use_network(self):
        """Test that services are connected to the network"""
        import yaml

        with open("docker-compose.yml") as f:
            config = yaml.safe_load(f)

        for service_name, service_config in config["services"].items():
            if "networks" in service_config:
                assert "idhub_network" in service_config["networks"]
