# Databricks notebook source
# MAGIC %md
# MAGIC # Mosaic AI Agent Framework: Author and deploy an MCP tool-calling LangGraph agent
# MAGIC
# MAGIC This notebook shows how to author a LangGraph agent that connects to MCP servers hosted on Databricks. You can choose between a Databricks-managed MCP server, a custom MCP server hosted as a Databricks app, or both simultaneously. To learn more about these options, see [MCP on Databricks](https://docs.databricks.com/aws/en/generative-ai/mcp/).
# MAGIC
# MAGIC
# MAGIC This notebook uses the [`ResponsesAgent`](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ResponsesAgent) interface for compatibility with Mosaic AI features. In this notebook you learn to:
# MAGIC
# MAGIC - Author a LangGraph agent (wrapped with `ResponsesAgent`) that calls MCP tools
# MAGIC - Manually test the agent
# MAGIC - Evaluate the agent using Mosaic AI Agent Evaluation
# MAGIC - Log and deploy the agent
# MAGIC
# MAGIC To learn more about authoring an agent using Mosaic AI Agent Framework, see Databricks documentation ([AWS](https://docs.databricks.com/aws/generative-ai/agent-framework/author-agent) | [Azure](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/create-chat-model)).
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.

# COMMAND ----------

# MAGIC %pip install -U -qqqq -r requirements.txt
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the agent code
# MAGIC
# MAGIC Define the agent code in a single cell below. This lets you easily write the agent code to a local Python file, using the `%%writefile` magic command, for subsequent logging and deployment.
# MAGIC
# MAGIC The following cell creates a flexible, tool-using agent that integrates Databricks MCP servers with the Mosaic AI Agent Framework. Here’s what happens, at a high level:
# MAGIC
# MAGIC 1. **MCP tool wrappers**  
# MAGIC    Custom wrapper classes are defined so LangChain tools can interact with Databricks MCP servers. You can connect to Databricks-managed MCP servers, custom MCP servers hosted as a Databricks App, or both.
# MAGIC
# MAGIC 2. **Tool discovery & registration**  
# MAGIC    The agent automatically discovers available tools from the specified MCP server(s), turns their schemas into Python objects, and prepares them for the LLM.
# MAGIC
# MAGIC 3. **Define the LangGraph agent logic**  
# MAGIC    Define an agent workflow that does the following:
# MAGIC    - The agent reads messages (conversation/history).
# MAGIC    - If a tool (function) call is requested, it’s executed using the correct MCP tool.
# MAGIC    - The agent can loop, performing multiple tool calls as needed, until a final answer is ready.
# MAGIC
# MAGIC 4. **Wrap the LangGraph agent using the `ResponsesAgent` class**  
# MAGIC    The agent is wrapped using `ResponsesAgent` so it's compatible with the Mosaic AI.
# MAGIC    
# MAGIC 5. **MLflow autotracing**
# MAGIC    Enable MLflow autologging to start automatic tracing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output and tool-calling abilities. Since this notebook called `mlflow.langchain.autolog()`, you can view the trace for each step the agent takes.

# COMMAND ----------

# ==============================================================================
# TODO: ONLY UNCOMMENT AND EDIT THIS SECTION IF YOU ARE USING OAUTH/SERVICE PRINCIPAL FOR CUSTOM MCP SERVERS.
#       For managed MCP (the default), LEAVE THIS SECTION COMMENTED OUT.
# ==============================================================================

import os

# # Set your Databricks client ID and client secret for service principal authentication.
DATABRICKS_CLIENT_ID = "17684918-4460-4eb7-b166-acc496563ae9"
DATABRICKS_CLIENT_SECRET = dbutils.secrets.get(scope="seatac", key="sp-agent-mcp-integration")

# # Load your service principal credentials into environment variables
os.environ["DATABRICKS_CLIENT_ID"] = DATABRICKS_CLIENT_ID
os.environ["DATABRICKS_CLIENT_SECRET"] = DATABRICKS_CLIENT_SECRET


# COMMAND ----------

from agent import AGENT

# COMMAND ----------

# from agent import AGENT

# AGENT.predict({"input": [{"role": "user", "content": "can you call tools until you find which jobs are in this workspace and what their tags are?"}]})

# COMMAND ----------

# for chunk in AGENT.predict_stream(
#     {"input": [{"role": "user", "content": "what jobs do I have and what are their tags?"}]}
# ):
#     print(chunk, "-----------\n")

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
# MAGIC ## Pre-deployment agent validation
# MAGIC Before registering and deploying the agent, perform pre-deployment checks using the [mlflow.models.predict()](https://mlflow.org/docs/latest/python_api/mlflow.models.html#mlflow.models.predict) API. See Databricks documentation ([AWS](https://docs.databricks.com/en/machine-learning/model-serving/model-serving-debug.html#validate-inputs) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/model-serving-debug#before-model-deployment-validation-checks)).

# COMMAND ----------

# mlflow.models.predict(
#     model_uri=f"runs:/{logged_agent_info.run_id}/agent",
#     input_data={"input": [{"role": "user", "content": "What is 7*6 in Python?"}]},
#     env_manager="uv",
# )

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
catalog = "buzzforce-demo-workspace_catalog"
schema = "buzzforce_clean"
model_name = "tag-agent"
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
    # ==============================================================================
    # TODO: ONLY UNCOMMENT AND CONFIGURE THE ENVIRONMENT_VARS SECTION BELOW
    #       IF YOU ARE USING OAUTH/SERVICE PRINCIPAL FOR CUSTOM MCP SERVERS.
    #       For managed MCP (the default), LEAVE THIS SECTION COMMENTED OUT.
    # ==============================================================================
    environment_vars={
    "DATABRICKS_CLIENT_ID": os.environ["DATABRICKS_CLIENT_ID"],
    "DATABRICKS_CLIENT_SECRET": os.environ["DATABRICKS_CLIENT_SECRET"]
    },
    tags = {"endpointSource": "docs"},
    endpoint_name='tag-agent'
)

