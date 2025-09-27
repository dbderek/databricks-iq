
TARGET=${1:-dev}

databricks bundle deploy -t $TARGET
databricks bundle run ui -t $TARGET &
databricks bundle run mcp -t $TARGET &
databricks bundle run databricks-iq-data-refresh -t $TARGET &
databricks bundle run databricks-iq-create-agent -t $TARGET &