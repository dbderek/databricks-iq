For many customers, monitoring the cost and performance of things such as serverless, jobs, model serving, and other processes is difficult. I propose an AI-driven solution that actively monitors these things and provides proactive solutions.

Proposed Solution.

I will build a combination of system table queries that curate usage and performance details and then I will create both an agent and a Databricks App that will act as the front-end/UI. The agent will be able to do things such as make recommendations in the App and also will have access to the Databricks SDK via MCP to aid with things like tagging resources. The entire solution will be deployed with a Databricks Asset Bundle with the goal of this being something a customer could put into their workspace to monitor the above-mentioned resources.

What solutions exist today to address the problem / opportunity statement.

Current solutions are mostly based around custom usage of system tables.

What does the final deliverable / impact for this project look like?

The final deliverable will be a neatly packaged DAB and will provide immediate observability for workspaces in a customer's account. It will also drive additional DBUs due to model serving and apps usage.