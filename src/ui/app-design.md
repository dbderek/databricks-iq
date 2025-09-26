I want to build a streamlit app to show data associated with costs within the Databricks platform. There is test data stored in the ./example_data folder, which is fine for initial development and matches the table names and schema of the actual data in Databricks SQL, but the data will be coming directly out of unity catalog tables using the Python SDK. I've provided an example of what reading data out of Databricks SQL looks like with the file example_databricks_sql.py. Build that option in to the application.


The app should be organized into neat pages with charts, tables, and predictive recommendations.

To start, I'd like to see the following pages. Note that you can add multiple tabs if you think it needs it. Try to keep the pages from getting too busy.
- Job spend metrics with filters available for workspace, user (run_as), tags, dates
- Serverless spend metrics with similar filtering capabilities
- Model serving and batch inference costs
- A chatbot interface using the model in ./src/agent/agent.py (I provided an example implementation in example_chatbot.py, you don't need to follow this exactly but use it for reference for interacting with the model.)

I've provided some design examples with databricks-design-guidelines.md and DatabricksLogo.png. Try to incorporate this branding into the app. I want the app to have a clean modern feel.