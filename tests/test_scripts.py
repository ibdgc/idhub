# tests/test_scripts.py
"""
Test utility scripts
"""

import os

import pytest


class TestScripts:
    """Test utility scripts"""

    def test_backup_script_exists(self):
        """Test that backup script exists"""
        assert os.path.exists("backup-idhub.sh")

    def test_backup_script_executable(self):
        """Test that backup script is executable"""
        if os.path.exists("backup-idhub.sh"):
            import stat

            st = os.stat("backup-idhub.sh")
            is_executable = bool(st.st_mode & stat.S_IXUSR)
            assert is_executable, "backup-idhub.sh should be executable"

    def test_sync_certs_script_exists(self):
        """Test that sync-certs script exists"""
        assert os.path.exists("sync-certs.sh")

    def test_sync_certs_script_executable(self):
        """Test that sync-certs script is executable"""
        if os.path.exists("sync-certs.sh"):
            import stat

            st = os.stat("sync-certs.sh")
            is_executable = bool(st.st_mode & stat.S_IXUSR)
            assert is_executable, "sync-certs.sh should be executable"

    def test_scripts_have_shebang(self):
        """Test that shell scripts have proper shebang"""
        scripts = ["backup-idhub.sh", "sync-certs.sh"]

        for script in scripts:
            if os.path.exists(script):
                with open(script) as f:
                    first_line = f.readline()

                assert first_line.startswith("#!"), (
                    f"{script} should start with shebang"
                )
                assert "bash" in first_line or "sh" in first_line, (
                    f"{script} should use bash or sh"
                )
