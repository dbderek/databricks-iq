"""
Databricks IQ MCP Server

A modular MCP server for managing Databricks resources with tagging and budget policies.
"""

from .clients.databricks_client import DatabricksClient
from .managers.tag_manager import TagManager
from .managers.budget_manager import BudgetManager

__version__ = "1.0.0"
__author__ = "Team BuzzForce"
__description__ = "MCP server for managing Databricks resource tags and budget policies"
__all__ = ["DatabricksClient", "TagManager", "BudgetManager"]
