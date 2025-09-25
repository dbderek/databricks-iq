# Databricks Tags Management Agent

This agent integrates with the Databricks Tags MCP server to provide intelligent tag management capabilities for Databricks resources.

## Features

The agent can help you with:
- **Cluster Tag Management**: List and update tags on Databricks clusters
- **SQL Warehouse Tag Management**: Manage tags on SQL warehouses  
- **Job Tag Management**: Handle tags for Databricks jobs
- **Pipeline Tag Management**: Manage tags on Delta Live Tables pipelines
- **Bulk Operations**: Update tags across multiple resources at once
- **Tag Discovery**: Find resources by their tags
- **Compliance Reporting**: Generate reports on missing required tags

## Prerequisites

1. **MCP Server Running**: Ensure the Databricks Tags MCP server is running and accessible
2. **Environment Variable**: Set `MCP_ENDPOINT` to point to your MCP server (defaults to `http://localhost:8000`)
3. **Databricks Authentication**: Ensure proper Databricks workspace authentication is configured

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set the MCP endpoint environment variable:
```bash
export MCP_ENDPOINT="http://your-mcp-server:8000"
```

3. Configure Databricks authentication (one of):
```bash
# Option 1: Environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-access-token"

# Option 2: Databricks CLI
databricks configure --token
```

## Usage

### In Databricks Notebook

Use `agent_driver.py` as a Databricks notebook to:
1. Test the agent interactively
2. Evaluate agent performance  
3. Log and register the agent model
4. Deploy the agent to a serving endpoint

### Direct Agent Usage

```python
from agent import AGENT

# Example: Get server status
response = AGENT.predict({
    "input": [{"type": "message", "role": "user", "content": "What's the status of the MCP server?"}]
})

# Example: List cluster tags
response = AGENT.predict({
    "input": [{"type": "message", "role": "user", "content": "Show me the tags for cluster 0123-456789-abcdef"}]
})

# Example: Update tags
response = AGENT.predict({
    "input": [{"type": "message", "role": "user", "content": "Add environment=production and team=data-eng tags to cluster 0123-456789-abcdef"}]
})
```

## Available Tools

The agent has access to all 16 MCP server tools:

### Server Management
- `get_server_status`: Check MCP server health and available tools

### Cluster Management  
- `list_cluster_tags`: List tags for a specific cluster
- `update_cluster_tags`: Update cluster tags
- `get_all_clusters_with_tags`: Get all clusters and their tags

### SQL Warehouse Management
- `list_warehouse_tags`: List tags for a specific warehouse
- `update_warehouse_tags`: Update warehouse tags  
- `get_all_warehouses_with_tags`: Get all warehouses and their tags

### Job Management
- `list_job_tags`: List tags for a specific job
- `update_job_tags`: Update job tags
- `get_all_jobs_with_tags`: Get all jobs and their tags

### Pipeline Management
- `list_pipeline_tags`: List tags for a specific pipeline
- `update_pipeline_tags`: Update pipeline tags
- `get_all_pipelines_with_tags`: Get all pipelines and their tags

### Bulk Operations
- `bulk_update_tags`: Update tags on multiple resources
- `find_resources_by_tag`: Find resources by tag key/value
- `tag_compliance_report`: Generate compliance reports

## Example Conversations

**"Show me all clusters and their current tags"**
- Agent will call `get_all_clusters_with_tags` and format the results

**"Add cost-center=eng-001 to cluster abc-123 and job 456"**  
- Agent will use `bulk_update_tags` for efficient multi-resource updates

**"Which resources are missing the 'environment' tag?"**
- Agent will call `tag_compliance_report` with required_tags=["environment"]

**"Find all production resources"**
- Agent will call `find_resources_by_tag` with tag_key="environment" and tag_value="production"

## Architecture

The agent uses:
- **LangGraph**: For conversation flow and tool orchestration
- **Databricks LLM**: Claude Sonnet 4 endpoint for natural language understanding
- **MCP Integration**: Direct HTTP calls to the Databricks Tags MCP server
- **ResponsesAgent**: Compatible with Databricks Agent Framework

## Environment Variables

- `MCP_ENDPOINT`: URL of the MCP server (default: `http://localhost:8000`)
- `DATABRICKS_HOST`: Databricks workspace URL
- `DATABRICKS_TOKEN`: Personal access token or service principal credentials

## Deployment

Use the `agent_driver.py` notebook in Databricks to:
1. Test and evaluate the agent
2. Log to MLflow with proper resource dependencies
3. Register in Unity Catalog
4. Deploy to Model Serving endpoint

The agent will automatically handle authentication and resource dependencies when deployed through the Databricks Agent Framework.
