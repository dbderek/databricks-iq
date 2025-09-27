# Databricks SQL Setup Guide

This guide explains how to configure Databricks IQ to connect to live Databricks SQL warehouses instead of using example CSV data.

## Environment Variables Setup

The application uses environment variables for secure and production-ready configuration:

### Required Environment Variables

```bash
# Databricks connection settings
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-databricks-token"
export SQL_WAREHOUSE="your-warehouse-id"
```

### Setting Environment Variables

#### Option 1: Command Line (Temporary)
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
export SQL_WAREHOUSE="1234567890abcdef"
```

#### Option 2: Environment File (Permanent)
Create a `.env` file in your project root:
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef
SQL_WAREHOUSE=1234567890abcdef
```

#### Option 3: Profile/Shell Configuration (Persistent)
Add to your `~/.bashrc`, `~/.zshrc`, or shell profile:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
export SQL_WAREHOUSE="1234567890abcdef"
```

## Getting Your Configuration Values

### 1. Databricks Host
- Go to your Databricks workspace
- Copy the URL (e.g., `https://your-workspace.cloud.databricks.com`)

### 2. Databricks Token
- In Databricks workspace, go to User Settings (click your profile picture)
- Navigate to "Developer" → "Access tokens"
- Click "Generate new token"
- Copy the generated token

### 3. SQL Warehouse ID
- In Databricks workspace, go to "SQL Warehouses" 
- Select your warehouse
- In the Connection Details tab, find the "Server hostname" and "HTTP path"
- The warehouse ID is the last part of the HTTP path: `/sql/1.0/warehouses/YOUR_WAREHOUSE_ID`

## Database Schema Setup

The application expects the following tables in your Databricks workspace:

### Catalog and Schema
- **Catalog:** `databricksiq`
- **Schema:** `main`

### Required Tables
1. `batch_inference_costs`
2. `failed_jobs_analysis`
3. `job_retry_patterns`
4. `job_spend_alerts`
5. `job_spend_trend`
6. `model_serving_costs`
7. `most_expensive_job_runs`
8. `most_expensive_jobs`
9. `serverless_consumption_by_tag`
10. `serverless_job_spend`
11. `serverless_notebook_spend`
12. `user_serverless_consumption`
13. `user_spend_alerts`
14. `workspace_spend_alerts`

### Table Creation
Use the notebooks in the `notebooks/databricks-iq-data/` folder to create the required tables:
- `create_jobs_analytics_mvws.ipynb`
- `create_serverless_user_analytics_mvws.ipynb`
- `create_serving_analytics_mvws.ipynb`

## Running the Application

1. **Set environment variables** (using one of the methods above)

2. **Verify configuration:**
   ```bash
   echo $DATABRICKS_HOST
   echo $DATABRICKS_TOKEN
   echo $SQL_WAREHOUSE
   ```

3. **Start the application:**
   ```bash
   cd src/ui
   streamlit run app.py
   ```

4. **Test connection:**
   - In the sidebar, select "Live Databricks SQL"
   - If configured correctly, you'll see: ✅ SQL Warehouse: your-warehouse-id
   - Click "🔍 Test Connection" to verify
   - Click "📋 List Available Tables" to see available tables

## Troubleshooting

### Common Issues

#### ❌ SQL_WAREHOUSE environment variable not set
**Solution:** Set the SQL_WAREHOUSE environment variable with your warehouse ID

#### ❌ Connection failed
**Possible causes:**
- Invalid DATABRICKS_HOST (check workspace URL)
- Invalid DATABRICKS_TOKEN (regenerate token)
- Invalid SQL_WAREHOUSE (check warehouse ID)
- Warehouse is stopped (start the warehouse in Databricks)

#### ⚠️ No tables found
**Possible causes:**
- Tables haven't been created yet (run setup notebooks)
- Wrong catalog/schema configuration
- Insufficient permissions

### Permissions Required
Your Databricks token needs:
- Read access to the `databricksiq.main` catalog/schema
- Usage permissions on the SQL warehouse
- Query execution permissions

## Security Best Practices

1. **Never commit tokens to git:**
   - Add `.env` to your `.gitignore`
   - Use environment variables instead of hardcoded values

2. **Token management:**
   - Generate tokens with minimal required permissions
   - Rotate tokens regularly
   - Revoke unused tokens

3. **Production deployment:**
   - Use secure secret management (AWS Secrets Manager, Azure Key Vault, etc.)
   - Implement token rotation
   - Monitor token usage

## Configuration Validation

The application automatically validates:
- Environment variable presence
- Connection to Databricks
- Table availability
- Query execution

Check the sidebar for real-time status indicators when using live data mode.