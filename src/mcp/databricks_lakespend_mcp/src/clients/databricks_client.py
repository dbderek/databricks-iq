"""Databricks workspace and account client wrapper."""

import os
import logging
from typing import Any, Dict, List, Optional
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.sql import EndpointInfo
from databricks.sdk.service.jobs import Job
from databricks.sdk.service.pipelines import PipelineStateInfo

logger = logging.getLogger(__name__)


class DatabricksClient:
    """Wrapper class for Databricks SDK operations."""
    
    def __init__(self):
        """Initialize the Databricks client with authentication."""
        try:
            self.client = WorkspaceClient()
            self.account_client = AccountClient() if self._can_initialize_account_client() else None
            logger.info("Databricks client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Databricks client: {e}")
            self.client = None
            self.account_client = None
    
    def _can_initialize_account_client(self) -> bool:
        """Check if account client can be initialized."""
        try:
            return os.environ.get('DATABRICKS_ACCOUNT_ID') is not None
        except:
            return False
    
    def test_connection(self) -> bool:
        """Test the connection to Databricks workspace."""
        try:
            if not self.client:
                return False
            
            # Try to get current user to test connection
            user = self.client.current_user.me()
            logger.info(f"Connected to Databricks as: {user.user_name}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current Databricks connection."""
        try:
            if not self.client:
                return {"status": "not_connected", "error": "Client not initialized"}
            
            user = self.client.current_user.me()
            workspace_info = self.client.workspace.get_status("/")
            
            return {
                "status": "connected",
                "user": user.user_name,
                "user_id": user.id,
                "workspace_id": getattr(workspace_info, 'object_id', 'unknown'),
                "host": os.environ.get('DATABRICKS_HOST', 'unknown')
            }
        except Exception as e:
            logger.error(f"Error getting connection info: {e}")
            return {"status": "error", "error": str(e)}

    # Cluster operations
    def get_cluster(self, cluster_id: str) -> Optional[ClusterDetails]:
        """Get cluster details by ID."""
        try:
            return self.client.clusters.get(cluster_id)
        except DatabricksError as e:
            logger.error(f"Error getting cluster {cluster_id}: {e}")
            raise
    
    def list_clusters(self) -> List[ClusterDetails]:
        """List all clusters in the workspace."""
        try:
            return list(self.client.clusters.list())
        except DatabricksError as e:
            logger.error(f"Error listing clusters: {e}")
            raise
    
    def edit_cluster(self, cluster_id: str, **kwargs) -> None:
        """Edit cluster configuration."""
        try:
            cluster = self.get_cluster(cluster_id)
            self.client.clusters.edit(
                cluster_id=cluster_id,
                cluster_name=cluster.cluster_name,
                spark_version=cluster.spark_version,
                node_type_id=cluster.node_type_id,
                **kwargs
            )
        except DatabricksError as e:
            logger.error(f"Error editing cluster {cluster_id}: {e}")
            raise

    # SQL Warehouse operations
    def get_warehouse(self, warehouse_id: str) -> Optional[EndpointInfo]:
        """Get SQL warehouse details by ID."""
        try:
            return self.client.warehouses.get(warehouse_id)
        except DatabricksError as e:
            logger.error(f"Error getting warehouse {warehouse_id}: {e}")
            raise
    
    def list_warehouses(self) -> List[EndpointInfo]:
        """List all SQL warehouses in the workspace."""
        try:
            return list(self.client.warehouses.list())
        except DatabricksError as e:
            logger.error(f"Error listing warehouses: {e}")
            raise
    
    def edit_warehouse(self, warehouse_id: str, **kwargs) -> None:
        """Edit SQL warehouse configuration."""
        try:
            warehouse = self.get_warehouse(warehouse_id)
            self.client.warehouses.edit(
                id=warehouse_id,
                name=warehouse.name,
                cluster_size=warehouse.cluster_size,
                **kwargs
            )
        except DatabricksError as e:
            logger.error(f"Error editing warehouse {warehouse_id}: {e}")
            raise

    # Job operations
    def get_job(self, job_id: int) -> Optional[Job]:
        """Get job details by ID."""
        try:
            return self.client.jobs.get(job_id)
        except DatabricksError as e:
            logger.error(f"Error getting job {job_id}: {e}")
            raise
    
    def list_jobs(self) -> List[Job]:
        """List all jobs in the workspace."""
        try:
            return list(self.client.jobs.list())
        except DatabricksError as e:
            logger.error(f"Error listing jobs: {e}")
            raise
    
    def update_job(self, job_id: int, **kwargs) -> None:
        """Update job configuration."""
        try:
            self.client.jobs.update(job_id=job_id, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating job {job_id}: {e}")
            raise

    # Pipeline operations
    def get_pipeline(self, pipeline_id: str) -> Optional[PipelineStateInfo]:
        """Get pipeline details by ID."""
        try:
            return self.client.pipelines.get(pipeline_id)
        except DatabricksError as e:
            logger.error(f"Error getting pipeline {pipeline_id}: {e}")
            raise
    
    def list_pipelines(self) -> List[PipelineStateInfo]:
        """List all pipelines in the workspace."""
        try:
            return list(self.client.pipelines.list_pipelines())
        except DatabricksError as e:
            logger.error(f"Error listing pipelines: {e}")
            raise
    
    def update_pipeline(self, pipeline_id: str, **kwargs) -> None:
        """Update pipeline configuration."""
        try:
            self.client.pipelines.update(pipeline_id=pipeline_id, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating pipeline {pipeline_id}: {e}")
            raise

    # Experiment operations
    def get_experiment(self, experiment_id: str):
        """Get experiment details by ID."""
        try:
            return self.client.experiments.get_experiment(experiment_id)
        except DatabricksError as e:
            logger.error(f"Error getting experiment {experiment_id}: {e}")
            raise
    
    def list_experiments(self):
        """List all experiments in the workspace."""
        try:
            return list(self.client.experiments.list_experiments())
        except DatabricksError as e:
            logger.error(f"Error listing experiments: {e}")
            raise
    
    def update_experiment(self, experiment_id: str, **kwargs) -> None:
        """Update experiment configuration."""
        try:
            self.client.experiments.update_experiment(experiment_id=experiment_id, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating experiment {experiment_id}: {e}")
            raise

    # Model Registry operations
    def get_model(self, model_name: str):
        """Get registered model details by name."""
        try:
            return self.client.model_registry.get_model(model_name)
        except DatabricksError as e:
            logger.error(f"Error getting model {model_name}: {e}")
            raise
    
    def list_models(self):
        """List all registered models in the workspace."""
        try:
            return list(self.client.model_registry.list_models())
        except DatabricksError as e:
            logger.error(f"Error listing models: {e}")
            raise
    
    def update_model(self, model_name: str, **kwargs) -> None:
        """Update registered model configuration."""
        try:
            self.client.model_registry.update_model(name=model_name, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating model {model_name}: {e}")
            raise

    # Unity Catalog operations
    def get_catalog(self, catalog_name: str):
        """Get catalog details by name."""
        try:
            return self.client.catalogs.get(catalog_name)
        except DatabricksError as e:
            logger.error(f"Error getting catalog {catalog_name}: {e}")
            raise
    
    def list_catalogs(self):
        """List all catalogs in the workspace."""
        try:
            return list(self.client.catalogs.list())
        except DatabricksError as e:
            logger.error(f"Error listing catalogs: {e}")
            raise
    
    def update_catalog(self, catalog_name: str, **kwargs) -> None:
        """Update catalog configuration."""
        try:
            self.client.catalogs.update(name=catalog_name, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating catalog {catalog_name}: {e}")
            raise

    def get_schema(self, schema_full_name: str):
        """Get schema details by full name."""
        try:
            return self.client.schemas.get(schema_full_name)
        except DatabricksError as e:
            logger.error(f"Error getting schema {schema_full_name}: {e}")
            raise
    
    def list_schemas(self, catalog_name: str):
        """List all schemas in a catalog."""
        try:
            return list(self.client.schemas.list(catalog_name))
        except DatabricksError as e:
            logger.error(f"Error listing schemas in catalog {catalog_name}: {e}")
            raise
    
    def update_schema(self, schema_full_name: str, **kwargs) -> None:
        """Update schema configuration."""
        try:
            self.client.schemas.update(name=schema_full_name, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating schema {schema_full_name}: {e}")
            raise

    def get_table(self, table_full_name: str):
        """Get table details by full name."""
        try:
            return self.client.tables.get(table_full_name)
        except DatabricksError as e:
            logger.error(f"Error getting table {table_full_name}: {e}")
            raise
    
    def list_tables(self, catalog_name: str, schema_name: str):
        """List all tables in a schema."""
        try:
            return list(self.client.tables.list(catalog_name, schema_name))
        except DatabricksError as e:
            logger.error(f"Error listing tables in {catalog_name}.{schema_name}: {e}")
            raise
    
    def update_table(self, table_full_name: str, **kwargs) -> None:
        """Update table configuration."""
        try:
            self.client.tables.update(name=table_full_name, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating table {table_full_name}: {e}")
            raise

    def get_volume(self, volume_full_name: str):
        """Get volume details by full name."""
        try:
            return self.client.volumes.read(volume_full_name)
        except DatabricksError as e:
            logger.error(f"Error getting volume {volume_full_name}: {e}")
            raise
    
    def list_volumes(self, catalog_name: str, schema_name: str):
        """List all volumes in a schema."""
        try:
            return list(self.client.volumes.list(catalog_name, schema_name))
        except DatabricksError as e:
            logger.error(f"Error listing volumes in {catalog_name}.{schema_name}: {e}")
            raise
    
    def update_volume(self, volume_full_name: str, **kwargs) -> None:
        """Update volume configuration."""
        try:
            self.client.volumes.update(name=volume_full_name, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating volume {volume_full_name}: {e}")
            raise

    # Repo operations
    def get_repo(self, repo_id: int):
        """Get repo details by ID."""
        try:
            return self.client.repos.get(repo_id)
        except DatabricksError as e:
            logger.error(f"Error getting repo {repo_id}: {e}")
            raise
    
    def list_repos(self):
        """List all repos in the workspace."""
        try:
            return list(self.client.repos.list())
        except DatabricksError as e:
            logger.error(f"Error listing repos: {e}")
            raise
    
    def update_repo(self, repo_id: int, **kwargs) -> None:
        """Update repo configuration."""
        try:
            self.client.repos.update(repo_id=repo_id, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating repo {repo_id}: {e}")
            raise

    # Serving endpoint operations
    def get_serving_endpoint(self, endpoint_name: str):
        """Get serving endpoint details by name."""
        try:
            return self.client.serving_endpoints.get(endpoint_name)
        except DatabricksError as e:
            logger.error(f"Error getting serving endpoint {endpoint_name}: {e}")
            raise
    
    def list_serving_endpoints(self):
        """List all serving endpoints in the workspace."""
        try:
            return list(self.client.serving_endpoints.list())
        except DatabricksError as e:
            logger.error(f"Error listing serving endpoints: {e}")
            raise
    
    def update_serving_endpoint(self, endpoint_name: str, **kwargs) -> None:
        """Update serving endpoint configuration."""
        try:
            self.client.serving_endpoints.update_config(name=endpoint_name, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating serving endpoint {endpoint_name}: {e}")
            raise

    # Budget policy operations (account-level)
    def get_budget_policy(self, policy_id: str):
        """Get budget policy details by ID."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            return self.account_client.budget_policy.get(policy_id)
        except DatabricksError as e:
            logger.error(f"Error getting budget policy {policy_id}: {e}")
            raise
    
    def list_budget_policies(self):
        """List all budget policies in the account."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            return list(self.account_client.budget_policy.list())
        except DatabricksError as e:
            logger.error(f"Error listing budget policies: {e}")
            raise
    
    def create_budget_policy(self, **kwargs):
        """Create a new budget policy."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            return self.account_client.budget_policy.create(**kwargs)
        except DatabricksError as e:
            logger.error(f"Error creating budget policy: {e}")
            raise
    
    def update_budget_policy(self, policy_id: str, **kwargs) -> None:
        """Update budget policy configuration."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            self.account_client.budget_policy.update(policy_id=policy_id, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating budget policy {policy_id}: {e}")
            raise
    
    def delete_budget_policy(self, policy_id: str) -> None:
        """Delete a budget policy."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            self.account_client.budget_policy.delete(policy_id)
        except DatabricksError as e:
            logger.error(f"Error deleting budget policy {policy_id}: {e}")
            raise

    # Budget operations (account-level)
    def get_budget(self, budget_id: str):
        """Get budget details by ID."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            return self.account_client.budgets.get(budget_id)
        except DatabricksError as e:
            logger.error(f"Error getting budget {budget_id}: {e}")
            raise
    
    def list_budgets(self):
        """List all budgets in the account."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            return list(self.account_client.budgets.list())
        except DatabricksError as e:
            logger.error(f"Error listing budgets: {e}")
            raise
    
    def create_budget(self, **kwargs):
        """Create a new budget."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            return self.account_client.budgets.create(**kwargs)
        except DatabricksError as e:
            logger.error(f"Error creating budget: {e}")
            raise
    
    def update_budget(self, budget_id: str, **kwargs) -> None:
        """Update budget configuration."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            self.account_client.budgets.update(budget_id=budget_id, **kwargs)
        except DatabricksError as e:
            logger.error(f"Error updating budget {budget_id}: {e}")
            raise
    
    def delete_budget(self, budget_id: str) -> None:
        """Delete a budget."""
        try:
            if not self.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            self.account_client.budgets.delete(budget_id)
        except DatabricksError as e:
            logger.error(f"Error deleting budget {budget_id}: {e}")
            raise