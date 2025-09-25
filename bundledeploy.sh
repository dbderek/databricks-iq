databricks bundle deploy -t dev
databricks bundle run ui -t dev &
databricks bundle run mcp -t dev &
databricks bundle run databricks_iq_data_refresh -t dev &
databricks bundle run agent_driver -t dev &