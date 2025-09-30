
TARGET=${1:-dev}
echo "🚀 Initializing Databricks deployment..."
echo "⏱️  This process may take up to 15 minutes to complete..."
echo "☕ Perfect time to grab a coffee while we work our magic! ✨"
echo "📝 Note: You will see streaming output from jobs and apps deployment"
sleep 1
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
databricks bundle deploy -t $TARGET
databricks bundle run ui -t $TARGET & 
databricks bundle run mcp -t $TARGET &
databricks bundle run databricks-lakespend-data-refresh -t $TARGET &
databricks bundle run databricks-lakespend-create-agent -t $TARGET &