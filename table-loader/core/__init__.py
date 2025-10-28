# table-loader/core/__init__.py
from .config import settings

# Import the class, not the instance
from .database import DatabaseManager

__all__ = ["settings", "DatabaseManager"]
