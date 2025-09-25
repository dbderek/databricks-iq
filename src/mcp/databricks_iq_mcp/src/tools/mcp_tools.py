"""MCP tools for Databricks resource management."""

import json
import logging
from typing import Any, Dict, List
from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def register_resource_tools(mcp: FastMCP, client, tag_manager, budget_manager):
    """Register all MCP tools for resource management."""
    
    # Connection and info tools
    @mcp.tool()
    def test_databricks_connection() -> str:
        """Test the connection to Databricks workspace."""
        success = client.test_connection()
        return f"Connection {'successful' if success else 'failed'}"

    @mcp.tool()
    def get_databricks_connection_info() -> Dict[str, Any]:
        """Get information about the current Databricks connection."""
        return client.get_connection_info()
    
    # Cluster management tools
    @mcp.tool()
    def list_clusters() -> List[Dict[str, Any]]:
        """List all clusters in the workspace."""
        clusters = client.list_clusters()
        return [
            {
                "cluster_id": cluster.cluster_id,
                "cluster_name": cluster.cluster_name,
                "state": cluster.state.value if cluster.state else "unknown",
                "node_type_id": cluster.node_type_id,
                "spark_version": cluster.spark_version,
                "tags": dict(cluster.custom_tags) if cluster.custom_tags else {}
            }
            for cluster in clusters
        ]

    @mcp.tool()
    def get_cluster_details(cluster_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific cluster."""
        cluster = client.get_cluster(cluster_id)
        return {
            "cluster_id": cluster.cluster_id,
            "cluster_name": cluster.cluster_name,
            "state": cluster.state.value if cluster.state else "unknown",
            "node_type_id": cluster.node_type_id,
            "spark_version": cluster.spark_version,
            "driver_node_type_id": cluster.driver_node_type_id,
            "num_workers": cluster.num_workers,
            "tags": dict(cluster.custom_tags) if cluster.custom_tags else {},
            "creator_user_name": cluster.creator_user_name,
            "start_time": cluster.start_time,
            "terminated_time": cluster.terminated_time
        }

    @mcp.tool()
    def set_cluster_tag(cluster_id: str, key: str, value: str) -> str:
        """Set a tag on a cluster."""
        success = tag_manager.set_cluster_tag(cluster_id, key, value)
        return f"Tag {'set' if success else 'failed to set'} on cluster {cluster_id}"

    @mcp.tool()
    def get_cluster_tags(cluster_id: str) -> Dict[str, str]:
        """Get all tags for a cluster."""
        return tag_manager.get_cluster_tags(cluster_id)

    @mcp.tool()
    def remove_cluster_tag(cluster_id: str, key: str) -> str:
        """Remove a tag from a cluster."""
        success = tag_manager.remove_cluster_tag(cluster_id, key)
        return f"Tag {'removed' if success else 'failed to remove'} from cluster {cluster_id}"
    
    # SQL Warehouse management tools
    @mcp.tool()
    def list_warehouses() -> List[Dict[str, Any]]:
        """List all SQL warehouses in the workspace."""
        warehouses = client.list_warehouses()
        return [
            {
                "id": warehouse.id,
                "name": warehouse.name,
                "state": warehouse.state.value if warehouse.state else "unknown",
                "cluster_size": warehouse.cluster_size,
                "auto_stop_mins": warehouse.auto_stop_mins,
                "tags": dict(warehouse.tags) if warehouse.tags else {}
            }
            for warehouse in warehouses
        ]

    @mcp.tool()
    def get_warehouse_details(warehouse_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific SQL warehouse."""
        warehouse = client.get_warehouse(warehouse_id)
        return {
            "id": warehouse.id,
            "name": warehouse.name,
            "state": warehouse.state.value if warehouse.state else "unknown",
            "cluster_size": warehouse.cluster_size,
            "auto_stop_mins": warehouse.auto_stop_mins,
            "max_num_clusters": warehouse.max_num_clusters,
            "min_num_clusters": warehouse.min_num_clusters,
            "tags": dict(warehouse.tags) if warehouse.tags else {},
            "creator_name": warehouse.creator_name,
            "jdbc_url": warehouse.jdbc_url
        }

    @mcp.tool()
    def set_warehouse_tag(warehouse_id: str, key: str, value: str) -> str:
        """Set a tag on a SQL warehouse."""
        success = tag_manager.set_warehouse_tag(warehouse_id, key, value)
        return f"Tag {'set' if success else 'failed to set'} on warehouse {warehouse_id}"

    @mcp.tool()
    def get_warehouse_tags(warehouse_id: str) -> Dict[str, str]:
        """Get all tags for a SQL warehouse."""
        return tag_manager.get_warehouse_tags(warehouse_id)

    @mcp.tool()
    def remove_warehouse_tag(warehouse_id: str, key: str) -> str:
        """Remove a tag from a SQL warehouse."""
        success = tag_manager.remove_warehouse_tag(warehouse_id, key)
        return f"Tag {'removed' if success else 'failed to remove'} from warehouse {warehouse_id}"
    
    # Job management tools
    @mcp.tool()
    def list_jobs() -> List[Dict[str, Any]]:
        """List all jobs in the workspace."""
        jobs = client.list_jobs()
        return [
            {
                "job_id": job.job_id,
                "name": job.settings.name if job.settings else "Unknown",
                "creator_user_name": job.creator_user_name,
                "created_time": job.created_time,
                "tags": dict(job.settings.tags) if job.settings and job.settings.tags else {}
            }
            for job in jobs
        ]

    @mcp.tool()
    def get_job_details(job_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific job."""
        job = client.get_job(job_id)
        return {
            "job_id": job.job_id,
            "name": job.settings.name if job.settings else "Unknown",
            "creator_user_name": job.creator_user_name,
            "created_time": job.created_time,
            "tags": dict(job.settings.tags) if job.settings and job.settings.tags else {},
            "max_concurrent_runs": job.settings.max_concurrent_runs if job.settings else None,
            "timeout_seconds": job.settings.timeout_seconds if job.settings else None,
            "schedule": job.settings.schedule.quartz_cron_expression if job.settings and job.settings.schedule else None
        }

    @mcp.tool()
    def set_job_tag(job_id: int, key: str, value: str) -> str:
        """Set a tag on a job."""
        success = tag_manager.set_job_tag(job_id, key, value)
        return f"Tag {'set' if success else 'failed to set'} on job {job_id}"

    @mcp.tool()
    def get_job_tags(job_id: int) -> Dict[str, str]:
        """Get all tags for a job."""
        return tag_manager.get_job_tags(job_id)

    @mcp.tool()
    def remove_job_tag(job_id: int, key: str) -> str:
        """Remove a tag from a job."""
        success = tag_manager.remove_job_tag(job_id, key)
        return f"Tag {'removed' if success else 'failed to remove'} from job {job_id}"
    
    # Pipeline management tools
    @mcp.tool()
    def list_pipelines() -> List[Dict[str, Any]]:
        """List all pipelines in the workspace."""
        pipelines = client.list_pipelines()
        return [
            {
                "pipeline_id": pipeline.pipeline_id,
                "name": pipeline.name,
                "state": pipeline.state.value if pipeline.state else "unknown",
                "creator_user_name": pipeline.creator_user_name,
                "tags": dict(pipeline.spec.custom_tags) if pipeline.spec and pipeline.spec.custom_tags else {}
            }
            for pipeline in pipelines
        ]

    @mcp.tool()
    def get_pipeline_details(pipeline_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific pipeline."""
        pipeline = client.get_pipeline(pipeline_id)
        return {
            "pipeline_id": pipeline.pipeline_id,
            "name": pipeline.name,
            "state": pipeline.state.value if pipeline.state else "unknown",
            "creator_user_name": pipeline.creator_user_name,
            "tags": dict(pipeline.spec.custom_tags) if pipeline.spec and pipeline.spec.custom_tags else {},
            "cluster_id": pipeline.cluster_id,
            "development": pipeline.spec.development if pipeline.spec else None,
            "continuous": pipeline.spec.continuous if pipeline.spec else None
        }

    @mcp.tool()
    def set_pipeline_tag(pipeline_id: str, key: str, value: str) -> str:
        """Set a tag on a pipeline."""
        success = tag_manager.set_pipeline_tag(pipeline_id, key, value)
        return f"Tag {'set' if success else 'failed to set'} on pipeline {pipeline_id}"

    @mcp.tool()
    def get_pipeline_tags(pipeline_id: str) -> Dict[str, str]:
        """Get all tags for a pipeline."""
        return tag_manager.get_pipeline_tags(pipeline_id)

    @mcp.tool()
    def remove_pipeline_tag(pipeline_id: str, key: str) -> str:
        """Remove a tag from a pipeline."""
        success = tag_manager.remove_pipeline_tag(pipeline_id, key)
        return f"Tag {'removed' if success else 'failed to remove'} from pipeline {pipeline_id}"
    
    # MLflow Experiment management tools
    @mcp.tool()
    def list_experiments() -> List[Dict[str, Any]]:
        """List all MLflow experiments in the workspace."""
        experiments = client.list_experiments()
        return [
            {
                "experiment_id": exp.experiment_id,
                "name": exp.name,
                "lifecycle_stage": exp.lifecycle_stage,
                "artifact_location": exp.artifact_location,
                "tags": dict(exp.tags) if hasattr(exp, 'tags') and exp.tags else {}
            }
            for exp in experiments
        ]

    @mcp.tool()
    def get_experiment_details(experiment_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific MLflow experiment."""
        experiment = client.get_experiment(experiment_id)
        return {
            "experiment_id": experiment.experiment_id,
            "name": experiment.name,
            "lifecycle_stage": experiment.lifecycle_stage,
            "artifact_location": experiment.artifact_location,
            "creation_time": experiment.creation_time,
            "last_update_time": experiment.last_update_time,
            "tags": dict(experiment.tags) if hasattr(experiment, 'tags') and experiment.tags else {}
        }

    @mcp.tool()
    def set_experiment_tag(experiment_id: str, key: str, value: str) -> str:
        """Set a tag on an MLflow experiment."""
        success = tag_manager.set_experiment_tag(experiment_id, key, value)
        return f"Tag {'set' if success else 'failed to set'} on experiment {experiment_id}"

    @mcp.tool()
    def get_experiment_tags(experiment_id: str) -> Dict[str, str]:
        """Get all tags for an MLflow experiment."""
        return tag_manager.get_experiment_tags(experiment_id)

    @mcp.tool()
    def remove_experiment_tag(experiment_id: str, key: str) -> str:
        """Remove a tag from an MLflow experiment."""
        success = tag_manager.remove_experiment_tag(experiment_id, key)
        return f"Tag {'removed' if success else 'failed to remove'} from experiment {experiment_id}"
    
    # Model Registry management tools
    @mcp.tool()
    def list_models() -> List[Dict[str, Any]]:
        """List all registered models in the workspace."""
        models = client.list_models()
        return [
            {
                "name": model.name,
                "description": model.description,
                "creation_timestamp": model.creation_timestamp,
                "last_updated_timestamp": model.last_updated_timestamp,
                "tags": dict(model.tags) if hasattr(model, 'tags') and model.tags else {}
            }
            for model in models
        ]

    @mcp.tool()
    def get_model_details(model_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific registered model."""
        model = client.get_model(model_name)
        return {
            "name": model.name,
            "description": model.description,
            "creation_timestamp": model.creation_timestamp,
            "last_updated_timestamp": model.last_updated_timestamp,
            "user_id": model.user_id,
            "tags": dict(model.tags) if hasattr(model, 'tags') and model.tags else {}
        }

    @mcp.tool()
    def set_model_tag(model_name: str, key: str, value: str) -> str:
        """Set a tag on a registered model."""
        success = tag_manager.set_model_tag(model_name, key, value)
        return f"Tag {'set' if success else 'failed to set'} on model {model_name}"

    @mcp.tool()
    def get_model_tags(model_name: str) -> Dict[str, str]:
        """Get all tags for a registered model."""
        return tag_manager.get_model_tags(model_name)

    @mcp.tool()
    def remove_model_tag(model_name: str, key: str) -> str:
        """Remove a tag from a registered model."""
        success = tag_manager.remove_model_tag(model_name, key)
        return f"Tag {'removed' if success else 'failed to remove'} from model {model_name}"


def register_unity_catalog_tools(mcp: FastMCP, client, tag_manager):
    """Register Unity Catalog specific tools."""
    
    # Catalog management tools
    @mcp.tool()
    def list_catalogs() -> List[Dict[str, Any]]:
        """List all catalogs in Unity Catalog."""
        catalogs = client.list_catalogs()
        return [
            {
                "name": catalog.name,
                "comment": catalog.comment,
                "metastore_id": catalog.metastore_id,
                "owner": catalog.owner,
                "properties": dict(catalog.properties) if catalog.properties else {}
            }
            for catalog in catalogs
        ]

    @mcp.tool()
    def get_catalog_details(catalog_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific catalog."""
        catalog = client.get_catalog(catalog_name)
        return {
            "name": catalog.name,
            "comment": catalog.comment,
            "metastore_id": catalog.metastore_id,
            "owner": catalog.owner,
            "properties": dict(catalog.properties) if catalog.properties else {},
            "created_at": catalog.created_at,
            "updated_at": catalog.updated_at,
            "created_by": catalog.created_by,
            "updated_by": catalog.updated_by
        }

    @mcp.tool()
    def set_catalog_tag(catalog_name: str, key: str, value: str) -> str:
        """Set a tag on a catalog."""
        success = tag_manager.set_catalog_tag(catalog_name, key, value)
        return f"Tag {'set' if success else 'failed to set'} on catalog {catalog_name}"

    @mcp.tool()
    def get_catalog_tags(catalog_name: str) -> Dict[str, str]:
        """Get all tags for a catalog."""
        return tag_manager.get_catalog_tags(catalog_name)

    @mcp.tool()
    def remove_catalog_tag(catalog_name: str, key: str) -> str:
        """Remove a tag from a catalog."""
        success = tag_manager.remove_catalog_tag(catalog_name, key)
        return f"Tag {'removed' if success else 'failed to remove'} from catalog {catalog_name}"
    
    # Schema management tools
    @mcp.tool()
    def list_schemas(catalog_name: str) -> List[Dict[str, Any]]:
        """List all schemas in a catalog."""
        schemas = client.list_schemas(catalog_name)
        return [
            {
                "name": schema.name,
                "full_name": schema.full_name,
                "comment": schema.comment,
                "owner": schema.owner,
                "properties": dict(schema.properties) if schema.properties else {}
            }
            for schema in schemas
        ]

    @mcp.tool()
    def get_schema_details(schema_full_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific schema."""
        schema = client.get_schema(schema_full_name)
        return {
            "name": schema.name,
            "full_name": schema.full_name,
            "comment": schema.comment,
            "owner": schema.owner,
            "properties": dict(schema.properties) if schema.properties else {},
            "created_at": schema.created_at,
            "updated_at": schema.updated_at,
            "created_by": schema.created_by,
            "updated_by": schema.updated_by
        }

    @mcp.tool()
    def set_schema_tag(schema_full_name: str, key: str, value: str) -> str:
        """Set a tag on a schema."""
        success = tag_manager.set_schema_tag(schema_full_name, key, value)
        return f"Tag {'set' if success else 'failed to set'} on schema {schema_full_name}"

    @mcp.tool()
    def get_schema_tags(schema_full_name: str) -> Dict[str, str]:
        """Get all tags for a schema."""
        return tag_manager.get_schema_tags(schema_full_name)

    @mcp.tool()
    def remove_schema_tag(schema_full_name: str, key: str) -> str:
        """Remove a tag from a schema."""
        success = tag_manager.remove_schema_tag(schema_full_name, key)
        return f"Tag {'removed' if success else 'failed to remove'} from schema {schema_full_name}"
    
    # Table management tools
    @mcp.tool()
    def list_tables(catalog_name: str, schema_name: str) -> List[Dict[str, Any]]:
        """List all tables in a schema."""
        tables = client.list_tables(catalog_name, schema_name)
        return [
            {
                "name": table.name,
                "full_name": table.full_name,
                "table_type": table.table_type.value if table.table_type else "unknown",
                "data_source_format": table.data_source_format.value if table.data_source_format else "unknown",
                "owner": table.owner,
                "comment": table.comment,
                "properties": dict(table.properties) if table.properties else {}
            }
            for table in tables
        ]

    @mcp.tool()
    def get_table_details(table_full_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific table."""
        table = client.get_table(table_full_name)
        return {
            "name": table.name,
            "full_name": table.full_name,
            "table_type": table.table_type.value if table.table_type else "unknown",
            "data_source_format": table.data_source_format.value if table.data_source_format else "unknown",
            "owner": table.owner,
            "comment": table.comment,
            "properties": dict(table.properties) if table.properties else {},
            "created_at": table.created_at,
            "updated_at": table.updated_at,
            "created_by": table.created_by,
            "updated_by": table.updated_by,
            "storage_location": table.storage_location
        }

    @mcp.tool()
    def set_table_tag(table_full_name: str, key: str, value: str) -> str:
        """Set a tag on a table."""
        success = tag_manager.set_table_tag(table_full_name, key, value)
        return f"Tag {'set' if success else 'failed to set'} on table {table_full_name}"

    @mcp.tool()
    def get_table_tags(table_full_name: str) -> Dict[str, str]:
        """Get all tags for a table."""
        return tag_manager.get_table_tags(table_full_name)

    @mcp.tool()
    def remove_table_tag(table_full_name: str, key: str) -> str:
        """Remove a tag from a table."""
        success = tag_manager.remove_table_tag(table_full_name, key)
        return f"Tag {'removed' if success else 'failed to remove'} from table {table_full_name}"
    
    # Volume management tools
    @mcp.tool()
    def list_volumes(catalog_name: str, schema_name: str) -> List[Dict[str, Any]]:
        """List all volumes in a schema."""
        volumes = client.list_volumes(catalog_name, schema_name)
        return [
            {
                "name": volume.name,
                "full_name": volume.full_name,
                "volume_type": volume.volume_type.value if volume.volume_type else "unknown",
                "owner": volume.owner,
                "comment": volume.comment
            }
            for volume in volumes
        ]

    @mcp.tool()
    def get_volume_details(volume_full_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific volume."""
        volume = client.get_volume(volume_full_name)
        return {
            "name": volume.name,
            "full_name": volume.full_name,
            "volume_type": volume.volume_type.value if volume.volume_type else "unknown",
            "owner": volume.owner,
            "comment": volume.comment,
            "created_at": volume.created_at,
            "updated_at": volume.updated_at,
            "created_by": volume.created_by,
            "updated_by": volume.updated_by,
            "storage_location": volume.storage_location
        }


def register_repo_tools(mcp: FastMCP, client, tag_manager):
    """Register Git repository management tools."""
    
    @mcp.tool()
    def list_repos() -> List[Dict[str, Any]]:
        """List all Git repositories in the workspace."""
        repos = client.list_repos()
        return [
            {
                "id": repo.id,
                "url": repo.url,
                "provider": repo.provider,
                "path": repo.path,
                "head_commit_id": repo.head_commit_id,
                "branch": repo.branch
            }
            for repo in repos
        ]

    @mcp.tool()
    def get_repo_details(repo_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific repository."""
        repo = client.get_repo(repo_id)
        return {
            "id": repo.id,
            "url": repo.url,
            "provider": repo.provider,
            "path": repo.path,
            "head_commit_id": repo.head_commit_id,
            "branch": repo.branch,
            "sparse_checkout": repo.sparse_checkout
        }


def register_serving_tools(mcp: FastMCP, client, tag_manager):
    """Register model serving endpoint tools."""
    
    @mcp.tool()
    def list_serving_endpoints() -> List[Dict[str, Any]]:
        """List all model serving endpoints in the workspace."""
        endpoints = client.list_serving_endpoints()
        return [
            {
                "name": endpoint.name,
                "creator": endpoint.creator,
                "creation_timestamp": endpoint.creation_timestamp,
                "last_updated_timestamp": endpoint.last_updated_timestamp,
                "state": endpoint.state.config_update if endpoint.state else "unknown",
                "tags": {tag.key: tag.value for tag in endpoint.tags} if endpoint.tags else {}
            }
            for endpoint in endpoints
        ]

    @mcp.tool()
    def get_serving_endpoint_details(endpoint_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific serving endpoint."""
        endpoint = client.get_serving_endpoint(endpoint_name)
        return {
            "name": endpoint.name,
            "creator": endpoint.creator,
            "creation_timestamp": endpoint.creation_timestamp,
            "last_updated_timestamp": endpoint.last_updated_timestamp,
            "state": endpoint.state.config_update if endpoint.state else "unknown",
            "tags": {tag.key: tag.value for tag in endpoint.tags} if endpoint.tags else {},
            "config": {
                "served_models": [
                    {
                        "name": model.name,
                        "model_name": model.model_name,
                        "model_version": model.model_version,
                        "workload_size": model.workload_size,
                        "scale_to_zero_enabled": model.scale_to_zero_enabled
                    }
                    for model in endpoint.config.served_models
                ] if endpoint.config and endpoint.config.served_models else []
            }
        }

    @mcp.tool()
    def set_serving_endpoint_tag(endpoint_name: str, key: str, value: str) -> str:
        """Set a tag on a serving endpoint."""
        success = tag_manager.set_serving_endpoint_tag(endpoint_name, key, value)
        return f"Tag {'set' if success else 'failed to set'} on serving endpoint {endpoint_name}"

    @mcp.tool()
    def get_serving_endpoint_tags(endpoint_name: str) -> Dict[str, str]:
        """Get all tags for a serving endpoint."""
        return tag_manager.get_serving_endpoint_tags(endpoint_name)

    @mcp.tool()
    def remove_serving_endpoint_tag(endpoint_name: str, key: str) -> str:
        """Remove a tag from a serving endpoint."""
        success = tag_manager.remove_serving_endpoint_tag(endpoint_name, key)
        return f"Tag {'removed' if success else 'failed to remove'} from serving endpoint {endpoint_name}"


def register_bulk_tools(mcp: FastMCP, tag_manager, budget_manager):
    """Register bulk operation and reporting tools."""
    
    @mcp.tool()
    def bulk_set_tags(resource_type: str, resource_ids_json: str, tags_json: str) -> Dict[str, Any]:
        """Set multiple tags on multiple resources of the same type."""
        try:
            resource_ids = json.loads(resource_ids_json)
            tags = json.loads(tags_json)
            results = tag_manager.bulk_set_tags(resource_type, resource_ids, tags)
            return {
                "success": True,
                "results": results,
                "summary": {
                    "total_resources": len(resource_ids),
                    "successful_resources": sum(1 for success in results.values() if success),
                    "failed_resources": sum(1 for success in results.values() if not success)
                }
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_compliance_report() -> Dict[str, Any]:
        """Generate a compliance report for all resources."""
        return tag_manager.get_compliance_report()

    @mcp.tool()
    def generate_budget_compliance_report() -> Dict[str, Any]:
        """Generate a budget policy compliance report for all resources."""
        return budget_manager.get_budget_compliance_report()


def register_budget_tools(mcp: FastMCP, budget_manager):
    """Register budget policy management tools."""
    
    @mcp.tool()
    def create_budget_policy(name: str, display_name: str, max_monthly_budget: float, 
                           alert_thresholds_json: str = None) -> str:
        """Create a new budget policy."""
        try:
            alert_thresholds = json.loads(alert_thresholds_json) if alert_thresholds_json else None
            policy_id = budget_manager.create_budget_policy(
                name, display_name, max_monthly_budget, alert_thresholds
            )
            return f"Budget policy created with ID: {policy_id}" if policy_id else "Failed to create budget policy"
        except Exception as e:
            return f"Error creating budget policy: {str(e)}"

    @mcp.tool()
    def list_budget_policies() -> List[Dict[str, Any]]:
        """List all budget policies."""
        return budget_manager.list_budget_policies()

    @mcp.tool()
    def get_budget_policy_details(policy_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific budget policy."""
        policy = budget_manager.get_budget_policy(policy_id)
        return policy if policy else {"error": "Budget policy not found"}

    @mcp.tool()
    def delete_budget_policy(policy_id: str) -> str:
        """Delete a budget policy."""
        success = budget_manager.delete_budget_policy(policy_id)
        return f"Budget policy {'deleted' if success else 'failed to delete'}"

    @mcp.tool()
    def apply_budget_policy_to_resource(resource_type: str, resource_id: str, 
                                       budget_policy_id: str) -> str:
        """Apply a budget policy to a specific resource."""
        success = budget_manager.apply_budget_policy_to_resource(
            resource_type, resource_id, budget_policy_id
        )
        return f"Budget policy {'applied' if success else 'failed to apply'} to {resource_type} {resource_id}"

    @mcp.tool()
    def get_resources_with_budget_policy(budget_policy_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get all resources that have a specific budget policy applied."""
        return budget_manager.get_resources_with_budget_policy(budget_policy_id)