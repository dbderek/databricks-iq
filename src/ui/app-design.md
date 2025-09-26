I want to build a streamlit app to show data associated with costs within the Databricks platform. There is test data stored in the ./example_data folder, which is fine for initial development and matches the table names and schema of the actual data in Databricks SQL, but the data will be coming directly out of unity catalog tables using the Python SDK. I've provided an example of what reading data out of Databricks SQL looks like with the file example_databricks_sql.py. Build that option in to the application.


The app should be organized into neat pages with charts, tables, and predictive recommendations.

To start, I'd like to see the following pages. Note that you can add multiple tabs if you think it needs it. Try to keep the pages from getting too busy.
- Job spend metrics with filters available for workspace, user (run_as), tags, dates
- Serverless spend metrics with similar filtering capabilities
- Model serving and batch inference costs
- A chatbot interface using the model in ./src/agent/agent.py (I provided an example implementation in example_chatbot.py, you don't need to follow this exactly but use it for reference for interacting with the model.)

I've provided some design examples with databricks-design-guidelines.md and DatabricksLogo.png. Try to incorporate this branding into the app. I want the app to have a clean modern feel.

------------------------------
### UPDATES TO APP:
I RECOMMEND SPLITTING ALL OF THE PAGES INTO THEIR OWN FILES FOR EASE OF MAINTENANCE

CHANGES TO PAGES
- Branding - make the databricks logo way smaller, if possible
- Overview - Remove this page
- Job Analytics - remove all existing charts and tables. make sure you include job_id and not just name because sometimes the name is null
    -- You need to be using the following tables, it might be best to make tabs for each of these
        --- most_expensive_jobs
        --- most_expensive_job_runs
        --- job_spend_trend
        --- failed_jobs_analysis
    -- Charts to Add (use your best judgement based on the file schema for what to show):
        --- I want cards showing totals and other metrics
        --- I want job spend by things such as job_id, User (run_as), tags, etc.
        --- I want a table showing the 100 most expensive jobs, ordered descending by cost
        --- I want a table showing the 100 most expensive job runs, ordered descending by cost
    -- Filters:
        --- User (run_as), Tag, date, job name, workspace id

- Serverless Analytics - remove all existing charts and tables. make sure you include job_id and not just name because sometimes the name is null
    -- You need to be using the following tables, it might be best to make tabs for each of these
        --- serverless_job_spend
        --- serverless_notebook_spend
        --- serverless_consumption_by_tag
    -- Charts to Add (use your best judgement based on the file schema for what to show):
        --- Use similar ideas to the Job Analytics page but use the table schema to come up with good charts
        --- In general, we want to see things by job or notebook, tag, User, date, etc.
    -- Filters:
        --- User (run_as), Tag, date

- Model Serving Analytics - remove all existing charts and tables and start over based on instructions below
    -- You need to be using the following tables, it might be best to make tabs for each of these
        --- model_serving_costs
        --- batch_inference_costs
    -- Charts to Add (use your best judgement based on the file schema for what to show):
        --- Use similar ideas to the other pages but use the table schema to come up with good charts
        --- In general, we want to see things by endpoint, tag, User (run_as), date, Owner (created_by)
    -- Filters:
        --- User (run_as), Tag, date, endpoint name

- [NEW PAGE] - User Analytics
    -- You need to be using the following tables, it might be best to make tabs for each of these
        --- user_serverless_consumption
        --- user_spend_alerts
    -- Charts to Add (use your best judgement based on the file schema for what to show):
        --- Use similar ideas to the other pages but use the table schema to come up with good charts and tables
        --- In general, we want to see things by tag, User (run_as), date, Owner (created_by)
    -- Filters:
        --- User (run_as)
    

EXPECTATIONS:
 - Any filters should be specific to the page and tab the user is on, ensure the column exists in the table for the filter 
 
------------------------------
 ### UPDATES TO APP 2:
GENERAL:
- The databricks logo is not showing up on any of the pages, it's just a broken link icon
- The chatbot needs to be refactored into it's own file
- If possible, remove the index column from all tables


CHANGES TO PAGES
- Job Analytics - add job_id filter to all of these tabs
    -- [Most Expensive Jobs Tab] Stack the charts "Top 10 Jobs by Cost" and "Top 10 Users by Job Cost" on top of each other and expand from top 10 to top 25. Make them both horizontal bar charts. Also add a bar chart showing the most job runs.
    -- [Most Expensive Job Runs Tab] Same feedback as previous tab. Stack charts, make both horizontal, expand to top 25. Also "Jobs with Most Runs" should be counts and not cost so there shouldn't be $ on the y-axis and make sure this is sorted descending.
    -- [Job Spend Trends Tab] - add cards showing T7D, T14D, and T30D costs. Show top 25 top jobs in terms of growth.
    -- [Failed Jobs Analysis Tab] - Show jobs with the most failures and highest costs associated with failures.
- Serverless Analytics
    -- [Serverless Job Spend Tab] Make "Top 10 Serverless Jobs by Cost" horizontal and top 25 instead of 10. Remove "DBU Usage vs. Cost". 
    -- [Serverless Notebook Spend Tab] Remove "Notebook Data Preview" table. Add horizontal bar chart showing top 25 notebook users (run_as.) in terms of cost.
    -- [Consumption by Tag Tab] This tab needs the ability to look at things by individual tags, which means the json in the custom_tags column needs to be unpacked and the data needs to be grouped by tags. There should also be a filter for tags.
- Model Serving Analytics
    -- [Model Serving Costs Tab] Remove "Cost Trends" chart. Move "Top 10 Endpoints by Cost" down below pie chart and make it horizontal bar chart with top 25 endpoints instead of 10. Add another pie Chart next to the existing pie chart that shows Cost distribution by endpoint_name.
    -- [Batch Inference Costs Tab] Remove "Batch Inference Data Review" table. Add a horizontal bar chart showing top 25 Users (run_as) by total costs. Add a stacked bar chart showing cost by day for the past 30 days where the bars are split by endpoint_name. 

- User Analytics - change the order of the tabs so "User Spend Alerts" is first.
    -- [User Spend Alerts Tab] - Rename tab to User Serverless Spend. Remove the "Spend Alerts Data Preview" table. Remove the "Top 10 Users by Alert Count" table. Add horizontal bar charts (full screen width) that show top 25 spending users for total, t7d, and t14d.
    -- [User Serverless Consumption Tab] Rename tab to User Serverless Spend Details. Remove both of the charts showing DBUs. Make the "Top 10 Users by Cost" chart a horizontal bar chart showing the top 25 instead of top 10. Put that next to the pie chart but split the columns so the bar chart has 75% of the width.
    