# tests/test_documentation.py
"""
Test documentation completeness
"""

import os

import pytest


class TestDocumentation:
    """Test documentation files"""

    def test_readme_not_empty(self):
        """Test that README.md is not empty"""
        with open("README.md") as f:
            content = f.read()

        assert len(content) > 100, "README.md appears to be empty or too short"

    def test_readme_has_title(self):
        """Test that README.md has a title"""
        with open("README.md") as f:
            content = f.read()

        assert content.startswith("#"), "README.md should start with a title"

    def test_readme_has_sections(self):
        """Test that README.md has key sections"""
        with open("README.md") as f:
            content = f.read().lower()

        # Check for common sections
        expected_sections = [
            "installation",
            "usage",
            "setup",
        ]

        # At least one of these should be present
        has_section = any(section in content for section in expected_sections)
        assert has_section, "README.md should have installation/usage/setup sections"

    @pytest.mark.parametrize(
        "service",
        [
            "gsid-service",
            "redcap-pipeline",
            "fragment-validator",
            "table-loader",
        ],
    )
    def test_service_has_readme_or_docstring(self, service):
        """Test that service has README or main module docstring"""
        readme_path = os.path.join(service, "README.md")
        main_path = os.path.join(service, "main.py")

        has_readme = os.path.exists(readme_path)
        has_main_docstring = False

        if os.path.exists(main_path):
            with open(main_path) as f:
                content = f.read()
                # Check for module-level docstring in first 1000 chars
                has_main_docstring = '"""' in content[:1000] or "'''" in content[:1000]

        # If neither exists, check if there's at least a Dockerfile with comments
        has_dockerfile_docs = False
        dockerfile_path = os.path.join(service, "Dockerfile")
        if os.path.exists(dockerfile_path):
            with open(dockerfile_path) as f:
                content = f.read()
                # Check for meaningful comments (not just auto-generated)
                has_dockerfile_docs = content.count("#") > 2

        assert has_readme or has_main_docstring or has_dockerfile_docs, (
            f"{service} should have README.md, documented main.py, or documented Dockerfile"
        )
