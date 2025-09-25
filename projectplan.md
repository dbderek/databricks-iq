# Databricks IQ Project Plan

## Overview
AI-driven cost and performance monitoring solution for Databricks workspaces using system tables, Databricks Apps, and MCP integration.

## Architecture
```
Data Layer: System table queries → Materialized views
Apps Layer: UI (Streamlit) + MCP Server (FastAPI)
Agent Layer: Claude integration via MCP
Deploy: Databricks Asset Bundle (DAB)
```

## Components

### 1. Data Pipeline (`resources/pipelines.yml`)
- **Status**: Configured ✅
- **Notebooks**: 
  - `create_jobs_analytics_mvws.ipynb`
  - `create_serverless_user_analytics_mvws.ipynb` 
  - `create_serving_analytics_mvws.ipynb`
- **Output**: Materialized views in `databricksiq.main` catalog/schema

### 2. MCP Server (`src/mcp/databricks_iq_mcp/`)
- **Status**: Implemented ✅
- **Features**: Tag management for clusters, warehouses, jobs, pipelines
- **API**: FastAPI server with web UI
- **SDK**: Databricks SDK integration

### 3. Databricks Apps (`resources/apps.yml`)
- **UI App**: `src/ui/` - Streamlit monitoring dashboard
- **MCP App**: Deployed MCP server
- **Status**: Configured, UI needs implementation

### 4. Agent Integration (`src/agent/`)
- **Status**: Configured ✅
- **Purpose**: Claude agent with MCP access for recommendations

## Implementation Tasks

### Phase 1: Data Foundation
- [ ] Verify materialized views creation
- [ ] Test data pipeline execution
- [ ] Validate system table queries

### Phase 2: UI Development
- [ ] Build Streamlit monitoring dashboard
- [ ] Connect to materialized views
- [ ] Implement cost/performance visualizations
- [ ] Add alerting capabilities

### Phase 3: Agent Integration
- [ ] Enhance MCP server with analytics endpoints
- [ ] Add recommendation logic
- [ ] Implement proactive monitoring alerts
- [ ] Test agent-driven workflows

### Phase 4: Deployment
- [ ] Complete DAB configuration
- [ ] Test multi-environment deployment
- [ ] Create deployment documentation

## File Structure
```
databricks-iq/
├── databricks.yml           # DAB configuration
├── resources/               # Resource definitions
│   ├── apps.yml            # Databricks Apps
│   ├── jobs.yml            # Scheduled jobs
│   └── pipelines.yml       # Data pipeline
├── notebooks/              # Data processing
│   └── databricks-iq-data/ # Analytics notebooks
├── src/
│   ├── ui/                 # Streamlit app
│   ├── mcp/                # MCP server
│   └── agent/              # Claude agent
└── example_data/           # Sample outputs
```

## Development Notes
- Use existing MCP server as foundation
- Leverage configured DAB structure
- Focus on clean, minimal code
- Prioritize working MVP over features