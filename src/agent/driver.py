# Databricks notebook source
# dbutils.widgets.text('sp_client_id', '', 'SP Client ID')
# dbutils.widgets.text('sp_secret_scope', '', 'SP Secret Scope')
# dbutils.widgets.text('sp_secret_key', '', 'SP Secret Key')
# dbutils.widgets.text('catalog', '', 'Catalog')
# dbutils.widgets.text('schema', '', 'Schema')

# COMMAND ----------

# MAGIC %pip install -U -qqqq -r requirements.txt
# MAGIC %restart_python

# COMMAND ----------

sp_client_id = dbutils.widgets.get('sp_client_id')
sp_secret_scope = dbutils.widgets.get('sp_secret_scope')
sp_secret_key = dbutils.widgets.get('sp_secret_key')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')

# COMMAND ----------

# ==============================================================================
# TODO: ONLY UNCOMMENT AND EDIT THIS SECTION IF YOU ARE USING OAUTH/SERVICE PRINCIPAL FOR CUSTOM MCP SERVERS.
#       For managed MCP (the default), LEAVE THIS SECTION COMMENTED OUT.
# ==============================================================================

import os

# # Set your Databricks client ID and client secret for service principal authentication.
DATABRICKS_CLIENT_ID = sp_client_id
DATABRICKS_CLIENT_SECRET = dbutils.secrets.get(scope=sp_secret_scope, key=sp_secret_key)
DATABRICKS_MCP_SERVER_URL = "https://<your-databricks-apps-instance>/mcp"

# # Load your service principal credentials into environment variables
os.environ["DATABRICKS_CLIENT_ID"] = DATABRICKS_CLIENT_ID
os.environ["DATABRICKS_CLIENT_SECRET"] = DATABRICKS_CLIENT_SECRET
os.environ["DATABRICKS_MCP_SERVER_URL"] = DATABRICKS_MCP_SERVER_URL


# COMMAND ----------

from agent import AGENT

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the agent as an MLflow model
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [Deploy an agent that connects to Databricks MCP servers](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp#deploy-your-agent).

# COMMAND ----------

import mlflow
from agent import LLM_ENDPOINT_NAME
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction
from pkg_resources import get_distribution

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME), 
    DatabricksFunction(function_name="system.ai.python_exec")
]

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name="tag_agent",
        python_model="agent.py",
        resources=resources,
        pip_requirements=[
            "databricks-mcp",
            f"mlflow=={get_distribution('mlflow').version}",
            f"langgraph=={get_distribution('langgraph').version}",
            f"mcp=={get_distribution('mcp').version}",
            f"databricks-langchain=={get_distribution('databricks-langchain').version}",
        ]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Before you deploy the agent, you must register the agent to Unity Catalog.
# MAGIC
# MAGIC - **TODO** Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
catalog = catalog
schema = schema
model_name = "databricks-lakespend-agent"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents

agents.deploy(
    UC_MODEL_NAME, 
    uc_registered_model_info.version,
    environment_vars={
    "DATABRICKS_CLIENT_ID": os.environ["DATABRICKS_CLIENT_ID"],
    "DATABRICKS_CLIENT_SECRET": os.environ["DATABRICKS_CLIENT_SECRET"],
    "DATABRICKS_MCP_SERVER_URL": os.environ["DATABRICKS_MCP_SERVER_URL"]
    },
    tags = {"endpointSource": "docs"},
    endpoint_name='dbx-iq-agent-endpoint'
)
