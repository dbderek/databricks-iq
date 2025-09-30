
# Databricks LakeSpend

Databricks LakeSpend is a comprehensive cost observability application designed to help organizations monitor, analyze, and optimize their Databricks workspace spending. The application provides detailed insights into resource usage, cost attribution, and budget management through an intuitive interface and powerful automation capabilities.

## 🎯 What is Databricks LakeSpend?

Databricks LakeSpend is a complete cost management solution that includes:

- **📊 Cost Analytics Dashboard**: Interactive Streamlit application for visualizing spending patterns across clusters, jobs, warehouses, and users
- **🏷️ Resource Tagging System**: Comprehensive MCP (Model Context Protocol) server for automated resource discovery and tag management
- **🤖 AI-Powered Assistant**: Intelligent agent for natural language cost queries and recommendations
- **📈 Usage Analytics**: Detailed tracking of compute resources, serverless usage, and model serving costs
- **💰 Budget Management**: Automated budget policies and cost alerting

## 🏗️ Architecture

The application consists of several integrated components:

- **Streamlit UI** (`src/ui/`): Web-based dashboard for cost visualization and analysis
- **MCP Server** (`src/mcp/`): Resource discovery and tag management server
- **AI Agent** (`src/agent/`): LLM-powered assistant for cost optimization insights
- **Data Pipelines** (`notebooks/`): ETL processes for cost data aggregation
- **Analytics Views**: Materialized views for performance optimization

## 🚀 Key Features

- **Multi-Resource Cost Tracking**: Monitor spending across all Databricks resource types (clusters, jobs, warehouses, ML endpoints, etc.)
- **User and Team Attribution**: Track costs by individual users, teams, and projects
- **Real-time Dashboards**: Live cost monitoring with customizable time ranges and filters
- **Automated Tagging**: Bulk resource tagging for improved cost attribution
- **Budget Policies**: Set and monitor spending limits with automated alerts
- **Cost Optimization Recommendations**: AI-powered suggestions for reducing costs

## 📋 Prerequisites

Before deploying Databricks LakeSpend, ensure you have:

- **Databricks Workspace**: Access to a Databricks workspace with admin privileges
- **Databricks CLI**: Latest version installed and configured (`pip install databricks-cli`)
- **Unity Catalog**: Enabled in your workspace (recommended)
- **Service Principal**: For automated operations and authentication

## 🚀 Quick Start

1. **Clone the repository** and configure your deployment settings
2. **Create a service principal** for the application 
3. **Deploy with Databricks Asset Bundles** using the provided scripts
4. **Access the dashboard** through your Databricks workspace

Detailed setup instructions are provided below.

## Setup Instructions
1. Create a service principal in your DABs target env, store the client_id, recommended sp name is _databricks-lakespend-sp_
2. Create a Databricks secret scope and key for the **client_secret** using the databricks CLI
    - Use `databricks secrets put-secret _scope_ _key_`
3. Create a databricks catalog and schema that you will use for the data and models.
    - Catalog: **databrickslakespend**
    - Schema: **main**
    - **IMPORTANT** Prior to running the DAB deployment in step 5, grant your service principal created in step 1 **ALL PRIVILEGES** on the catalog you create.
4. Configure the DAB deployment in databricks.yml (modify databricks.yml.template)
    - Update variables: 
        - catalog: destination catalog, recommend using default "databrickslakespend"
        - schema: destination schema, recommend using default "main"
        - service_principal_name: name of sp created in step 1
        - sp_client_id: client_id of sp created in step 1
        - sp_secret_scope: secret scope created in step 2
        - sp_secret_key: secret key created in step 2
        - dbsql_serverless_warehouse_id: id of sql warehouse that app will have access to
5. In your terminal run the following CLI commands
    - `cd /path/to/your/project`
    - `bundledeploy.sh _target_` 
6. After successful deployment, configure the following permissions in the target workspace:
    - **MAKE SURE RESOURCES ARE FINISHED PROVISIONING PRIOR TO FOLLOWING THESE NEXT STEPS**
    - Grant the service principal created in step 1 **CAN_USE** permission on the mcp server application, **mcp-databricks-lakespend**.
    - Grant the **databricks-lakespend** application service principal **CAN_QUERY** access on the agent endpoint, **dbx-lakespend-agent-endpoint**.
    - Grant the **databricks-lakespend** application service principal **USE_CATALOG**, **USE_SCHEMA**, **SELECT**, **EXECUTE** permissions on the catalog defined in the DABs variable (likely **databrickslakespend**).
    -- **Important**: The mcp server needs to have the right permissions to be able to read and apply tags, as well as create and attach budget policies. If you do not want to grant admin privileges to the mcp server service principal, you'll need to explicity define assets you want the server to have permissions to. 

### Cleanup Instructions
1. In the Databricks workspace you want to remove the project from, run the notebook that is stored at `./cleanup/cleanup_databricks_lakespend_model`.
    - Input the appropriate model and endpoint names in the parameter boxes
2. In your terminal, run the following CLI commands
    - `cd /path/to/your/project`
    - `databricks bundle destroy -t _your_target_name_`

## 📊 What You Get

After deployment, you'll have access to:

- **Cost Analytics Dashboard**: Navigate to your Databricks workspace and find the "databricks-lakespend" application
- **MCP Server**: REST API for programmatic resource management and tagging
- **AI Assistant**: Chat interface for natural language cost queries
- **Automated Data Pipelines**: Regular cost data aggregation and analysis
- **Budget Monitoring**: Proactive alerts when spending thresholds are exceeded

## 🎯 Use Cases

Databricks LakeSpend is ideal for:

- **FinOps Teams**: Implementing cost governance and chargeback models
- **Platform Engineers**: Monitoring resource utilization and optimization opportunities
- **Data Teams**: Understanding the cost impact of their workflows and experiments
- **Executives**: Getting high-level visibility into Databricks spending trends

## 🤝 Support

For questions, issues, or feature requests, please refer to the documentation in each component directory:
- UI Documentation: `src/ui/README.md`
- MCP Server Documentation: `src/mcp/databricks_lakespend_mcp/README.md`
- Agent Documentation: `src/agent/README.md`

## 📄 License

This project is designed for deployment within your own Databricks workspace using Databricks Asset Bundles (DABs).
