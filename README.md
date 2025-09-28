
### Setup Instructions
1. Create a service principal in your DABs target env, store the client_id, recommended sp name is _databricks-iq-sp_
2. Create a Databricks secret scope and key for the **client_secret**
    - Use `databricks secrets put-secret _scope_ _key_`
3. Configure the DAB deployment in databricks.yml 
    - Update variables: 
        - catalog: destination catalog, recommend using default "databricksiq"
        - schema: destination schema, recommend using default "main"
        - service_principal_name: name of sp created in step 1
        - sp_client_id: client_id of sp created in step 1
        - sp_secret_scope: secret scope created in step 2
        - sp_secret_key: secret key created in step 2
        - dbsql_serverless_warehouse_id: id of sql warehouse that app will have access to
4. Run bundledeploy.sh _target_ to deploy the DAB
5. After successful deployment, configure the following permissions in the target workspace:
    - **MAKE SURE RESOURCES ARE FINISHED PROVISIONING PRIOR TO FOLLOWING THESE NEXT STEPS**
    - Grant the service principal created in step 1 **CAN_USE** permission on the mcp server application, **mcp-databricks-iq**.
    - Grant the **databricks-iq** application service principal **CAN_QUERY** access on the agent endpoint, **dbx-iq-agent-endpoint**.
    - Grant the **databricks-iq** application service principal **USE_CATALOG**, **USE_SCHEMA**, **SELECT**, **EXECUTE** permissions on the catalog defined in the DABs variable (likely **databricksiq**).
    -- **Important**: The mcp server needs to have the rigth permissions to be able to read and apply tags, as well as create and attach budget policies. If you do not want to grant admin privileges to the mcp server service principal, you'll need to explicity define assets you want the server to have permissions to. 
