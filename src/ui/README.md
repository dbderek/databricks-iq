# Databricks LakeSpend - UI Component

A comprehensive Streamlit dashboard for Databricks cost management and resource analytics.

## Features

### 📊 Cost Analytics Dashboard
- **Job Spend Metrics**: Analyze job costs with filtering by workspace, owner, and tags
- **Serverless Analytics**: Monitor serverless compute consumption and costs
- **Model Serving Costs**: Track model serving and batch inference expenses
- **Interactive Visualizations**: Charts, graphs, and trend analysis using Plotly

### 🏷️ Resource Tagging Assistant
- **AI-Powered Chatbot**: Natural language interface for resource management
- **Comprehensive Tagging**: Manage tags across 12+ Databricks resource types
- **Bulk Operations**: Apply tags to multiple resources simultaneously
- **Compliance Reporting**: Generate tag compliance reports

### 🎨 Databricks Design System
- **Official Branding**: Databricks colors, fonts, and design guidelines
- **Responsive Layout**: Works on desktop and mobile devices
- **Modern Interface**: Clean, professional appearance

## Installation

### Prerequisites
- Python 3.8+
- Access to Databricks workspace (for live data)
- Databricks CLI configured (optional, for authentication)

### Install Dependencies
```bash
# Navigate to the UI directory
cd src/ui/

# Create virtual environment
python3 -m venv streamlit-env

# Install required packages
./streamlit-env/bin/python3 -m pip install -r requirements.txt
```

### Run the Application
```bash
# Run the Streamlit application using the virtual environment
./streamlit-env/bin/python3 -m streamlit run app.py
```

The application will be available at: http://localhost:8501

## Configuration

### Data Sources
The application supports two data source modes:

1. **Example Data** (default): Uses CSV files from `/example_data/` within the UI directory
2. **Live Databricks Data**: Connects to your Databricks SQL warehouse

### Databricks Connection
To use live data:
1. Select "Live Databricks Data" in the sidebar
2. Enter your SQL Warehouse HTTP Path (format: `/sql/1.0/warehouses/your-warehouse-id`)
3. Ensure you have proper authentication configured

### Environment Variables
```bash
# For the chatbot (optional)
export SERVING_ENDPOINT=your-agent-endpoint

# For Databricks authentication (if not using CLI)
export DATABRICKS_HOST=your-workspace-url
export DATABRICKS_TOKEN=your-access-token
```

## Usage

### Navigation
The application includes five main sections:

1. **Overview**: High-level cost metrics and trends
2. **Job Analytics**: Detailed job spend analysis with filters
3. **Serverless Analytics**: Serverless compute consumption insights
4. **Model Serving**: Model serving and inference cost tracking
5. **Resource Assistant**: AI chatbot for resource tagging

### Filtering Options
- **Workspace ID**: Filter by specific workspace
- **Owner**: Filter by resource owner (run_as user)
- **Tags**: Filter by custom tag keys and values
- **Date Ranges**: 7-day, 14-day, and 30-day periods

### Key Metrics
- Total spend across all resource types
- Growth rates and trend analysis
- Cost breakdowns by category
- Top spenders and resource utilization
- Compliance and tagging coverage

## File Structure
```
src/ui/
├── app.py                 # Main Streamlit application
├── example_chatbot.py     # Chatbot interface with agent integration
├── requirements.txt       # Python dependencies
├── config.py             # Application configuration
└── README.md             # This file
```

## Data Schema

### Example Data Files
The application works with several CSV data sources:

- `job_spend_trend.csv`: Job cost trends and growth metrics
- `serverless_job_spend.csv`: Serverless compute costs
- `model_serving_costs.csv`: Model serving endpoint costs
- `user_serverless_consumption.csv`: User-level consumption data
- `workspace_spend_alerts.csv`: Spending alert configurations

### Custom Tags Format
Tags are stored in JSON format within the CSV files:
```json
{
  "owner": "data-team",
  "environment": "production", 
  "cost_center": "eng-001",
  "project": "customer-analytics"
}
```

## Features in Detail

### Cost Analytics
- **Multi-dimensional Analysis**: Slice data by workspace, owner, tags, time periods
- **Visual Charts**: Interactive Plotly visualizations with drill-down capabilities
- **Trend Analysis**: Growth rates, spending patterns, and forecasting
- **Export Options**: Download filtered data and reports

### Resource Tagging Chatbot
- **Natural Language**: Ask questions in plain English
- **MCP Integration**: Connects to the Model Context Protocol server
- **Bulk Operations**: "Tag all production jobs with cost_center=eng-001"
- **Compliance**: "Show me all untagged clusters"
- **Best Practices**: Get guidance on tagging strategies

### Databricks Integration
- **Live Data**: Direct connection to Databricks SQL warehouses
- **Authentication**: Supports multiple auth methods (CLI, environment vars)
- **Real-time**: Fresh data from your Databricks workspace
- **Secure**: Uses official Databricks SDK and connectors

## Customization

### Adding New Visualizations
1. Create new functions in `app.py`
2. Follow the existing pattern with metric cards and Plotly charts
3. Use the Databricks color scheme for consistency

### Custom Filters
1. Add filter controls in the sidebar using `st.sidebar.multiselect()`
2. Apply filters to dataframes using pandas operations
3. Update all charts and tables with filtered data

### Branding Updates
1. Modify the `DATABRICKS_CSS` constant for styling changes
2. Update color schemes in Plotly chart configurations
3. Add custom logos or images as needed

## Troubleshooting

### Common Issues
1. **Import Errors**: Ensure all dependencies are installed (`pip install -r requirements.txt`)
2. **Connection Failures**: Verify Databricks authentication and network access
3. **Data Loading**: Check that example CSV files exist in the correct location
4. **Agent Offline**: Ensure the MCP server is running and properly configured

### Performance Optimization
- Use `@st.cache_data` for expensive data operations
- Limit data queries with appropriate filters
- Consider pagination for large datasets

### Development Mode
```bash
# Run with auto-reload for development
streamlit run app.py --server.runOnSave true
```

## Integration with Other Components

### MCP Server Integration
- The chatbot connects to the MCP server at `/src/mcp/databricks_lakespend_mcp/`
- Uses the comprehensive tagging and budget management tools
- Supports all 12+ Databricks resource types

### Agent Integration  
- Leverages the LangGraph agent from `/src/agent/agent.py`
- Provides natural language interface for complex operations
- Includes comprehensive system prompting for resource management

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the example data and configurations
3. Ensure all dependencies and authentication are properly configured
4. Verify the MCP server and agent are operational

The Databricks LakeSpend dashboard provides a comprehensive solution for cost management and resource analytics across your Databricks environment.