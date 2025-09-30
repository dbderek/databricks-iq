# Databricks LakeSpend MCP Server

A Model Context Protocol (MCP) server implementation using FastMCP that provides comprehensive tag management tools for Databricks resources including clusters, SQL warehouses, jobs, and pipelines.

## Features

This MCP server provides the following tools for Databricks tag management:

### Cluster Tag Management
- **list_cluster_tags**: List all tags for a specific cluster
- **update_cluster_tags**: Update tags on a cluster (add, modify, or remove)
- **get_all_clusters_with_tags**: Get all clusters and their current tags

### SQL Warehouse Tag Management  
- **list_warehouse_tags**: List all tags for a specific SQL warehouse
- **update_warehouse_tags**: Update tags on a SQL warehouse
- **get_all_warehouses_with_tags**: Get all SQL warehouses and their current tags

### Job Tag Management
- **list_job_tags**: List all tags for a specific job
- **update_job_tags**: Update tags on a job
- **get_all_jobs_with_tags**: Get all jobs and their current tags

### Pipeline Tag Management
- **list_pipeline_tags**: List all tags for a specific pipeline
- **update_pipeline_tags**: Update tags on a pipeline  
- **get_all_pipelines_with_tags**: Get all pipelines and their current tags

### Bulk Operations
- **bulk_update_tags**: Update tags across multiple resources at once
- **find_resources_by_tag**: Find all resources that have specific tag keys or values
- **tag_compliance_report**: Generate a report on tag compliance across resources

## Web Interface

The project includes a modern, responsive web interface that allows you to interact with all Databricks tag management tools through your browser. The interface features:

- 🎨 **Modern Design**: Clean, responsive UI with Databricks branding
- 🔄 **Real-time Status**: Live server status monitoring  
- 🛠️ **All Tools Available**: Access to all tag management tools
- 📱 **Mobile Friendly**: Works great on desktop and mobile devices
- 🏷️ **Tag Management**: Intuitive tag editing and bulk operations
- 📊 **Compliance Reports**: Visual tag compliance dashboards

## Prerequisites

- Python 3.7+
- Databricks workspace access
- Valid Databricks authentication (token or OAuth)

## Installation

1. Clone or download this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Set up your Databricks authentication:

```bash
# Option 1: Environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-access-token"

# Option 2: Databricks CLI profile  
databricks configure --token
```

## Usage

### Web Interface (Recommended)

The easiest way to use the MCP server is through the web interface:

```bash
# Launch the web server
python src/main.py
```

Then open your browser to: **http://localhost:8000**

### Traditional MCP Server

For MCP protocol integration:

```bash
# Launch the MCP server
python src/app.py
```

### Configuration

The server supports multiple authentication methods:

1. **Environment Variables**: Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
2. **Databricks CLI Profile**: Use configured CLI profile
3. **Runtime Configuration**: Pass credentials via the web interface

## Example Tool Usage

Once connected via an MCP client, you can use the available tools:

### Cluster Tag Management
```python
# List cluster tags
list_cluster_tags(cluster_id="0123-456789-abcdef")

# Update cluster tags
update_cluster_tags(
    cluster_id="0123-456789-abcdef",
    tags={"environment": "production", "team": "data-eng", "cost-center": "eng-001"}
)
```

### Job Tag Management  
```python
# List job tags
list_job_tags(job_id=123456)

# Update job tags
update_job_tags(
    job_id=123456,
    tags={"owner": "john.doe", "project": "ml-pipeline", "criticality": "high"}
)
```

### Bulk Operations
```python
# Find resources by tag
find_resources_by_tag(tag_key="environment", tag_value="production")

# Bulk update tags
bulk_update_tags(
    resources=[
        {"type": "cluster", "id": "cluster-1"},
        {"type": "job", "id": "job-123"}
    ],
    tags={"compliance": "updated", "last-reviewed": "2025-08-21"}
)
```

## Development

### Project Structure

```
databricks_tags_mcp/
├── src/
│   ├── __init__.py           # Package initialization
│   ├── app.py                # Main MCP server implementation
│   ├── main.py               # Web server entry point
│   ├── databricks_client.py  # Databricks SDK client wrapper
│   ├── tag_manager.py        # Tag management business logic
│   └── static/               # Web interface files
│       ├── index.html        # Main HTML interface
│       ├── styles.css        # CSS styling
│       └── script.js         # JavaScript functionality
├── requirements.txt          # Python dependencies
├── README.md                 # This file
└── app.yaml                  # Deployment configuration
```

### Adding New Tools

To add new tag management tools:

1. Add the business logic to `tag_manager.py`
2. Create a new tool function in `app.py` with the `@mcp.tool()` decorator
3. Add proper type hints and docstring
4. The tool will automatically be available via the MCP protocol

### Error Handling

The server includes comprehensive error handling for:
- Invalid Databricks credentials
- Resource not found errors
- API rate limiting
- Network connectivity issues
- Invalid tag formats

## Security Considerations

- Never expose Databricks tokens in logs or client-side code
- Use environment variables or secure credential storage
- Implement proper access controls for production deployments
- Monitor tag changes for compliance and auditing

## Requirements

- Python 3.7+
- FastMCP library
- Databricks SDK
- FastAPI
- Asyncio support

## License

This project is part of the SeaTac Data Quality Monitoring System. Use responsibly with proper Databricks authentication.

## Troubleshooting

### Common Issues

1. **Authentication errors**: Verify your Databricks token and workspace URL
2. **Permission denied**: Ensure your token has the necessary permissions for resource management
3. **Resource not found**: Check that cluster/job/pipeline IDs are correct and accessible
4. **API rate limits**: The server includes automatic retry logic with exponential backoff

### Getting Help

If you encounter issues:

1. Check that Databricks authentication is properly configured
2. Verify your token has cluster, job, and pipeline management permissions
3. Check the server logs for detailed error messages
4. Test connectivity to your Databricks workspace manually
