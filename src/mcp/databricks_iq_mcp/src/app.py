"""
Databricks IQ MCP Server - Refactored Main Application

A Model Context Protocol (MCP) server for managing Databricks resources.
This server provides tools for tagging and budget policy management across
multiple Databricks asset types including clusters, warehouses, jobs, 
pipelines, experiments, models, Unity Catalog resources, repositories,
and serving endpoints.
"""

import os
import logging
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastmcp import FastMCP

from .clients.databricks_client import DatabricksClient
from .managers.tag_manager import TagManager
from .managers.budget_manager import BudgetManager
from .tools.mcp_tools import (
    register_resource_tools,
    register_unity_catalog_tools,
    register_repo_tools,
    register_serving_tools,
    register_bulk_tools,
    register_budget_tools
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP("Databricks IQ MCP Server")

# Global instances
databricks_client = None
tag_manager = None
budget_manager = None

def initialize_clients():
    """Initialize Databricks clients and managers."""
    global databricks_client, tag_manager, budget_manager
    
    try:
        databricks_client = DatabricksClient()
        tag_manager = TagManager(databricks_client)
        budget_manager = BudgetManager(databricks_client)
        logger.info("Databricks clients initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize clients: {e}")
        return False

# Initialize clients
if not initialize_clients():
    logger.warning("Client initialization failed - some features may not be available")

# Register all MCP tools
if databricks_client and tag_manager and budget_manager:
    try:
        register_resource_tools(mcp, databricks_client, tag_manager, budget_manager)
        register_unity_catalog_tools(mcp, databricks_client, tag_manager)
        register_repo_tools(mcp, databricks_client, tag_manager)
        register_serving_tools(mcp, databricks_client, tag_manager)
        register_bulk_tools(mcp, tag_manager, budget_manager)
        register_budget_tools(mcp, budget_manager)
        logger.info("All MCP tools registered successfully")
    except Exception as e:
        logger.error(f"Failed to register MCP tools: {e}")

# Create the MCP app
mcp_app = mcp.streamable_http_app()

# Create the main FastAPI app
app = FastAPI(
    title="Databricks IQ MCP Server",
    description="A comprehensive MCP server for managing Databricks resource tags and budget policies",
    version="1.0.0",
    lifespan=lambda _: mcp.session_manager.run(),
)

STATIC_DIR = Path(__file__).parent / "static"

@app.get("/", include_in_schema=False)
async def serve_index():
    """Serve the main web interface."""
    return FileResponse(STATIC_DIR / "index.html")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        connection_status = databricks_client.test_connection() if databricks_client else False
        return {
            "status": "healthy" if connection_status else "unhealthy",
            "databricks_connection": connection_status,
            "account_client_available": databricks_client.account_client is not None if databricks_client else False,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# Mount the MCP app
app.mount("/", mcp_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)