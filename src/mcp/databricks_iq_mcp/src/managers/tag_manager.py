"""Tag management for Databricks resources."""

import logging
from typing import Any, Dict, List, Optional
from ..clients.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class TagManager:
    """Manages tags for Databricks resources."""
    
    def __init__(self, client: DatabricksClient):
        """Initialize TagManager with Databricks client."""
        self.client = client
        if not client or not client.client:
            raise ValueError("Valid Databricks client is required")
    
    def set_tag(self, resource_type: str, resource_id: str, key: str, value: str) -> bool:
        """Set a tag on a resource."""
        try:
            if resource_type == "cluster":
                return self.set_cluster_tag(resource_id, key, value)
            elif resource_type == "warehouse":
                return self.set_warehouse_tag(resource_id, key, value)
            elif resource_type == "job":
                return self.set_job_tag(int(resource_id), key, value)
            elif resource_type == "pipeline":
                return self.set_pipeline_tag(resource_id, key, value)
            elif resource_type == "experiment":
                return self.set_experiment_tag(resource_id, key, value)
            elif resource_type == "model":
                return self.set_model_tag(resource_id, key, value)
            elif resource_type == "catalog":
                return self.set_catalog_tag(resource_id, key, value)
            elif resource_type == "schema":
                return self.set_schema_tag(resource_id, key, value)
            elif resource_type == "table":
                return self.set_table_tag(resource_id, key, value)
            elif resource_type == "volume":
                return self.set_volume_tag(resource_id, key, value)
            elif resource_type == "repo":
                return self.set_repo_tag(int(resource_id), key, value)
            elif resource_type == "serving_endpoint":
                return self.set_serving_endpoint_tag(resource_id, key, value)
            else:
                logger.error(f"Unsupported resource type: {resource_type}")
                return False
        except Exception as e:
            logger.error(f"Error setting tag on {resource_type} {resource_id}: {e}")
            return False
    
    def set_cluster_tag(self, cluster_id: str, key: str, value: str) -> bool:
        """Set a tag on a cluster."""
        try:
            cluster = self.client.get_cluster(cluster_id)
            custom_tags = dict(cluster.custom_tags) if cluster.custom_tags else {}
            custom_tags[key] = value
            
            self.client.edit_cluster(cluster_id, custom_tags=custom_tags)
            logger.info(f"Added tag {key}={value} to cluster {cluster_id}")
            return True
        except Exception as e:
            logger.error(f"Error setting cluster tag: {e}")
            return False
    
    def set_warehouse_tag(self, warehouse_id: str, key: str, value: str) -> bool:
        """Set a tag on a SQL warehouse."""
        try:
            warehouse = self.client.get_warehouse(warehouse_id)
            tags = dict(warehouse.tags) if warehouse.tags else {}
            tags[key] = value
            
            self.client.edit_warehouse(warehouse_id, tags=tags)
            logger.info(f"Added tag {key}={value} to warehouse {warehouse_id}")
            return True
        except Exception as e:
            logger.error(f"Error setting warehouse tag: {e}")
            return False
    
    def set_job_tag(self, job_id: int, key: str, value: str) -> bool:
        """Set a tag on a job."""
        try:
            job = self.client.get_job(job_id)
            tags = dict(job.settings.tags) if job.settings and job.settings.tags else {}
            tags[key] = value
            
            self.client.update_job(job_id, tags=tags)
            logger.info(f"Added tag {key}={value} to job {job_id}")
            return True
        except Exception as e:
            logger.error(f"Error setting job tag: {e}")
            return False
    
    def set_pipeline_tag(self, pipeline_id: str, key: str, value: str) -> bool:
        """Set a tag on a pipeline."""
        try:
            pipeline = self.client.get_pipeline(pipeline_id)
            custom_tags = dict(pipeline.spec.custom_tags) if pipeline.spec and pipeline.spec.custom_tags else {}
            custom_tags[key] = value
            
            self.client.update_pipeline(pipeline_id, custom_tags=custom_tags)
            logger.info(f"Added tag {key}={value} to pipeline {pipeline_id}")
            return True
        except Exception as e:
            logger.error(f"Error setting pipeline tag: {e}")
            return False
    
    def set_experiment_tag(self, experiment_id: str, key: str, value: str) -> bool:
        """Set a tag on an experiment."""
        try:
            self.client.client.experiments.set_experiment_tag(experiment_id, key, value)
            logger.info(f"Added tag {key}={value} to experiment {experiment_id}")
            return True
        except Exception as e:
            logger.error(f"Error setting experiment tag: {e}")
            return False
    
    def set_model_tag(self, model_name: str, key: str, value: str) -> bool:
        """Set a tag on a registered model."""
        try:
            self.client.client.model_registry.set_model_tag(model_name, key, value)
            logger.info(f"Added tag {key}={value} to model {model_name}")
            return True
        except Exception as e:
            logger.error(f"Error setting model tag: {e}")
            return False
    
    def set_catalog_tag(self, catalog_name: str, key: str, value: str) -> bool:
        """Set a tag on a catalog."""
        try:
            catalog = self.client.get_catalog(catalog_name)
            properties = dict(catalog.properties) if catalog.properties else {}
            properties[key] = value
            
            self.client.update_catalog(catalog_name, properties=properties)
            logger.info(f"Added tag {key}={value} to catalog {catalog_name}")
            return True
        except Exception as e:
            logger.error(f"Error setting catalog tag: {e}")
            return False
    
    def set_schema_tag(self, schema_full_name: str, key: str, value: str) -> bool:
        """Set a tag on a schema."""
        try:
            schema = self.client.get_schema(schema_full_name)
            properties = dict(schema.properties) if schema.properties else {}
            properties[key] = value
            
            self.client.update_schema(schema_full_name, properties=properties)
            logger.info(f"Added tag {key}={value} to schema {schema_full_name}")
            return True
        except Exception as e:
            logger.error(f"Error setting schema tag: {e}")
            return False
    
    def set_table_tag(self, table_full_name: str, key: str, value: str) -> bool:
        """Set a tag on a table."""
        try:
            table = self.client.get_table(table_full_name)
            properties = dict(table.properties) if table.properties else {}
            properties[key] = value
            
            self.client.update_table(table_full_name, properties=properties)
            logger.info(f"Added tag {key}={value} to table {table_full_name}")
            return True
        except Exception as e:
            logger.error(f"Error setting table tag: {e}")
            return False
    
    def set_volume_tag(self, volume_full_name: str, key: str, value: str) -> bool:
        """Set a tag on a volume."""
        try:
            # Volumes don't have a direct tagging mechanism like other resources
            # This would need to be implemented based on specific requirements
            logger.warning(f"Volume tagging not fully implemented for {volume_full_name}")
            return False
        except Exception as e:
            logger.error(f"Error setting volume tag: {e}")
            return False
    
    def set_repo_tag(self, repo_id: int, key: str, value: str) -> bool:
        """Set a tag on a repo."""
        try:
            # Repos don't have a direct tagging mechanism in the current SDK
            # This would need to be implemented based on specific requirements
            logger.warning(f"Repo tagging not fully implemented for {repo_id}")
            return False
        except Exception as e:
            logger.error(f"Error setting repo tag: {e}")
            return False
    
    def set_serving_endpoint_tag(self, endpoint_name: str, key: str, value: str) -> bool:
        """Set a tag on a serving endpoint."""
        try:
            endpoint = self.client.get_serving_endpoint(endpoint_name)
            tags = list(endpoint.tags) if endpoint.tags else []
            
            # Remove existing tag with same key if it exists
            tags = [tag for tag in tags if tag.key != key]
            
            # Add new tag
            from databricks.sdk.service.serving import EndpointTag
            tags.append(EndpointTag(key=key, value=value))
            
            self.client.update_serving_endpoint(endpoint_name, tags=tags)
            logger.info(f"Added tag {key}={value} to serving endpoint {endpoint_name}")
            return True
        except Exception as e:
            logger.error(f"Error setting serving endpoint tag: {e}")
            return False
    
    def get_tags(self, resource_type: str, resource_id: str) -> Dict[str, str]:
        """Get all tags for a resource."""
        try:
            if resource_type == "cluster":
                return self.get_cluster_tags(resource_id)
            elif resource_type == "warehouse":
                return self.get_warehouse_tags(resource_id)
            elif resource_type == "job":
                return self.get_job_tags(int(resource_id))
            elif resource_type == "pipeline":
                return self.get_pipeline_tags(resource_id)
            elif resource_type == "experiment":
                return self.get_experiment_tags(resource_id)
            elif resource_type == "model":
                return self.get_model_tags(resource_id)
            elif resource_type == "catalog":
                return self.get_catalog_tags(resource_id)
            elif resource_type == "schema":
                return self.get_schema_tags(resource_id)
            elif resource_type == "table":
                return self.get_table_tags(resource_id)
            elif resource_type == "volume":
                return self.get_volume_tags(resource_id)
            elif resource_type == "repo":
                return self.get_repo_tags(int(resource_id))
            elif resource_type == "serving_endpoint":
                return self.get_serving_endpoint_tags(resource_id)
            else:
                logger.error(f"Unsupported resource type: {resource_type}")
                return {}
        except Exception as e:
            logger.error(f"Error getting tags for {resource_type} {resource_id}: {e}")
            return {}
    
    def get_cluster_tags(self, cluster_id: str) -> Dict[str, str]:
        """Get all tags for a cluster."""
        try:
            cluster = self.client.get_cluster(cluster_id)
            return dict(cluster.custom_tags) if cluster.custom_tags else {}
        except Exception as e:
            logger.error(f"Error getting cluster tags: {e}")
            return {}
    
    def get_warehouse_tags(self, warehouse_id: str) -> Dict[str, str]:
        """Get all tags for a SQL warehouse."""
        try:
            warehouse = self.client.get_warehouse(warehouse_id)
            return dict(warehouse.tags) if warehouse.tags else {}
        except Exception as e:
            logger.error(f"Error getting warehouse tags: {e}")
            return {}
    
    def get_job_tags(self, job_id: int) -> Dict[str, str]:
        """Get all tags for a job."""
        try:
            job = self.client.get_job(job_id)
            return dict(job.settings.tags) if job.settings and job.settings.tags else {}
        except Exception as e:
            logger.error(f"Error getting job tags: {e}")
            return {}
    
    def get_pipeline_tags(self, pipeline_id: str) -> Dict[str, str]:
        """Get all tags for a pipeline."""
        try:
            pipeline = self.client.get_pipeline(pipeline_id)
            return dict(pipeline.spec.custom_tags) if pipeline.spec and pipeline.spec.custom_tags else {}
        except Exception as e:
            logger.error(f"Error getting pipeline tags: {e}")
            return {}
    
    def get_experiment_tags(self, experiment_id: str) -> Dict[str, str]:
        """Get all tags for an experiment."""
        try:
            experiment = self.client.get_experiment(experiment_id)
            return dict(experiment.tags) if hasattr(experiment, 'tags') and experiment.tags else {}
        except Exception as e:
            logger.error(f"Error getting experiment tags: {e}")
            return {}
    
    def get_model_tags(self, model_name: str) -> Dict[str, str]:
        """Get all tags for a registered model."""
        try:
            model = self.client.get_model(model_name)
            return dict(model.tags) if hasattr(model, 'tags') and model.tags else {}
        except Exception as e:
            logger.error(f"Error getting model tags: {e}")
            return {}
    
    def get_catalog_tags(self, catalog_name: str) -> Dict[str, str]:
        """Get all tags for a catalog."""
        try:
            catalog = self.client.get_catalog(catalog_name)
            return dict(catalog.properties) if catalog.properties else {}
        except Exception as e:
            logger.error(f"Error getting catalog tags: {e}")
            return {}
    
    def get_schema_tags(self, schema_full_name: str) -> Dict[str, str]:
        """Get all tags for a schema."""
        try:
            schema = self.client.get_schema(schema_full_name)
            return dict(schema.properties) if schema.properties else {}
        except Exception as e:
            logger.error(f"Error getting schema tags: {e}")
            return {}
    
    def get_table_tags(self, table_full_name: str) -> Dict[str, str]:
        """Get all tags for a table."""
        try:
            table = self.client.get_table(table_full_name)
            return dict(table.properties) if table.properties else {}
        except Exception as e:
            logger.error(f"Error getting table tags: {e}")
            return {}
    
    def get_volume_tags(self, volume_full_name: str) -> Dict[str, str]:
        """Get all tags for a volume."""
        try:
            # Volumes don't have a direct tagging mechanism
            logger.warning(f"Volume tag retrieval not fully implemented for {volume_full_name}")
            return {}
        except Exception as e:
            logger.error(f"Error getting volume tags: {e}")
            return {}
    
    def get_repo_tags(self, repo_id: int) -> Dict[str, str]:
        """Get all tags for a repo."""
        try:
            # Repos don't have a direct tagging mechanism in the current SDK
            logger.warning(f"Repo tag retrieval not fully implemented for {repo_id}")
            return {}
        except Exception as e:
            logger.error(f"Error getting repo tags: {e}")
            return {}
    
    def get_serving_endpoint_tags(self, endpoint_name: str) -> Dict[str, str]:
        """Get all tags for a serving endpoint."""
        try:
            endpoint = self.client.get_serving_endpoint(endpoint_name)
            tags = {}
            if endpoint.tags:
                for tag in endpoint.tags:
                    tags[tag.key] = tag.value
            return tags
        except Exception as e:
            logger.error(f"Error getting serving endpoint tags: {e}")
            return {}
    
    def remove_tag(self, resource_type: str, resource_id: str, key: str) -> bool:
        """Remove a tag from a resource."""
        try:
            if resource_type == "cluster":
                return self.remove_cluster_tag(resource_id, key)
            elif resource_type == "warehouse":
                return self.remove_warehouse_tag(resource_id, key)
            elif resource_type == "job":
                return self.remove_job_tag(int(resource_id), key)
            elif resource_type == "pipeline":
                return self.remove_pipeline_tag(resource_id, key)
            elif resource_type == "experiment":
                return self.remove_experiment_tag(resource_id, key)
            elif resource_type == "model":
                return self.remove_model_tag(resource_id, key)
            elif resource_type == "catalog":
                return self.remove_catalog_tag(resource_id, key)
            elif resource_type == "schema":
                return self.remove_schema_tag(resource_id, key)
            elif resource_type == "table":
                return self.remove_table_tag(resource_id, key)
            elif resource_type == "volume":
                return self.remove_volume_tag(resource_id, key)
            elif resource_type == "repo":
                return self.remove_repo_tag(int(resource_id), key)
            elif resource_type == "serving_endpoint":
                return self.remove_serving_endpoint_tag(resource_id, key)
            else:
                logger.error(f"Unsupported resource type: {resource_type}")
                return False
        except Exception as e:
            logger.error(f"Error removing tag from {resource_type} {resource_id}: {e}")
            return False
    
    def remove_cluster_tag(self, cluster_id: str, key: str) -> bool:
        """Remove a tag from a cluster."""
        try:
            cluster = self.client.get_cluster(cluster_id)
            custom_tags = dict(cluster.custom_tags) if cluster.custom_tags else {}
            
            if key in custom_tags:
                del custom_tags[key]
                self.client.edit_cluster(cluster_id, custom_tags=custom_tags)
                logger.info(f"Removed tag {key} from cluster {cluster_id}")
                return True
            else:
                logger.warning(f"Tag {key} not found on cluster {cluster_id}")
                return False
        except Exception as e:
            logger.error(f"Error removing cluster tag: {e}")
            return False
    
    def remove_warehouse_tag(self, warehouse_id: str, key: str) -> bool:
        """Remove a tag from a SQL warehouse."""
        try:
            warehouse = self.client.get_warehouse(warehouse_id)
            tags = dict(warehouse.tags) if warehouse.tags else {}
            
            if key in tags:
                del tags[key]
                self.client.edit_warehouse(warehouse_id, tags=tags)
                logger.info(f"Removed tag {key} from warehouse {warehouse_id}")
                return True
            else:
                logger.warning(f"Tag {key} not found on warehouse {warehouse_id}")
                return False
        except Exception as e:
            logger.error(f"Error removing warehouse tag: {e}")
            return False
    
    def remove_job_tag(self, job_id: int, key: str) -> bool:
        """Remove a tag from a job."""
        try:
            job = self.client.get_job(job_id)
            tags = dict(job.settings.tags) if job.settings and job.settings.tags else {}
            
            if key in tags:
                del tags[key]
                self.client.update_job(job_id, tags=tags)
                logger.info(f"Removed tag {key} from job {job_id}")
                return True
            else:
                logger.warning(f"Tag {key} not found on job {job_id}")
                return False
        except Exception as e:
            logger.error(f"Error removing job tag: {e}")
            return False
    
    def remove_pipeline_tag(self, pipeline_id: str, key: str) -> bool:
        """Remove a tag from a pipeline."""
        try:
            pipeline = self.client.get_pipeline(pipeline_id)
            custom_tags = dict(pipeline.spec.custom_tags) if pipeline.spec and pipeline.spec.custom_tags else {}
            
            if key in custom_tags:
                del custom_tags[key]
                self.client.update_pipeline(pipeline_id, custom_tags=custom_tags)
                logger.info(f"Removed tag {key} from pipeline {pipeline_id}")
                return True
            else:
                logger.warning(f"Tag {key} not found on pipeline {pipeline_id}")
                return False
        except Exception as e:
            logger.error(f"Error removing pipeline tag: {e}")
            return False
    
    def remove_experiment_tag(self, experiment_id: str, key: str) -> bool:
        """Remove a tag from an experiment."""
        try:
            self.client.client.experiments.delete_experiment_tag(experiment_id, key)
            logger.info(f"Removed tag {key} from experiment {experiment_id}")
            return True
        except Exception as e:
            logger.error(f"Error removing experiment tag: {e}")
            return False
    
    def remove_model_tag(self, model_name: str, key: str) -> bool:
        """Remove a tag from a registered model."""
        try:
            self.client.client.model_registry.delete_model_tag(model_name, key)
            logger.info(f"Removed tag {key} from model {model_name}")
            return True
        except Exception as e:
            logger.error(f"Error removing model tag: {e}")
            return False
    
    def remove_catalog_tag(self, catalog_name: str, key: str) -> bool:
        """Remove a tag from a catalog."""
        try:
            catalog = self.client.get_catalog(catalog_name)
            properties = dict(catalog.properties) if catalog.properties else {}
            
            if key in properties:
                del properties[key]
                self.client.update_catalog(catalog_name, properties=properties)
                logger.info(f"Removed tag {key} from catalog {catalog_name}")
                return True
            else:
                logger.warning(f"Tag {key} not found on catalog {catalog_name}")
                return False
        except Exception as e:
            logger.error(f"Error removing catalog tag: {e}")
            return False
    
    def remove_schema_tag(self, schema_full_name: str, key: str) -> bool:
        """Remove a tag from a schema."""
        try:
            schema = self.client.get_schema(schema_full_name)
            properties = dict(schema.properties) if schema.properties else {}
            
            if key in properties:
                del properties[key]
                self.client.update_schema(schema_full_name, properties=properties)
                logger.info(f"Removed tag {key} from schema {schema_full_name}")
                return True
            else:
                logger.warning(f"Tag {key} not found on schema {schema_full_name}")
                return False
        except Exception as e:
            logger.error(f"Error removing schema tag: {e}")
            return False
    
    def remove_table_tag(self, table_full_name: str, key: str) -> bool:
        """Remove a tag from a table."""
        try:
            table = self.client.get_table(table_full_name)
            properties = dict(table.properties) if table.properties else {}
            
            if key in properties:
                del properties[key]
                self.client.update_table(table_full_name, properties=properties)
                logger.info(f"Removed tag {key} from table {table_full_name}")
                return True
            else:
                logger.warning(f"Tag {key} not found on table {table_full_name}")
                return False
        except Exception as e:
            logger.error(f"Error removing table tag: {e}")
            return False
    
    def remove_volume_tag(self, volume_full_name: str, key: str) -> bool:
        """Remove a tag from a volume."""
        try:
            # Volumes don't have a direct tagging mechanism
            logger.warning(f"Volume tag removal not fully implemented for {volume_full_name}")
            return False
        except Exception as e:
            logger.error(f"Error removing volume tag: {e}")
            return False
    
    def remove_repo_tag(self, repo_id: int, key: str) -> bool:
        """Remove a tag from a repo."""
        try:
            # Repos don't have a direct tagging mechanism in the current SDK
            logger.warning(f"Repo tag removal not fully implemented for {repo_id}")
            return False
        except Exception as e:
            logger.error(f"Error removing repo tag: {e}")
            return False
    
    def remove_serving_endpoint_tag(self, endpoint_name: str, key: str) -> bool:
        """Remove a tag from a serving endpoint."""
        try:
            endpoint = self.client.get_serving_endpoint(endpoint_name)
            tags = list(endpoint.tags) if endpoint.tags else []
            
            # Remove tag with the specified key
            tags = [tag for tag in tags if tag.key != key]
            
            self.client.update_serving_endpoint(endpoint_name, tags=tags)
            logger.info(f"Removed tag {key} from serving endpoint {endpoint_name}")
            return True
        except Exception as e:
            logger.error(f"Error removing serving endpoint tag: {e}")
            return False
    
    def bulk_set_tags(self, resource_type: str, resource_ids: List[str], tags: Dict[str, str]) -> Dict[str, bool]:
        """Set multiple tags on multiple resources."""
        results = {}
        
        for resource_id in resource_ids:
            resource_results = {}
            for key, value in tags.items():
                success = self.set_tag(resource_type, resource_id, key, value)
                resource_results[key] = success
            
            # Overall success for this resource
            results[resource_id] = all(resource_results.values())
        
        return results
    
    def get_compliance_report(self) -> Dict[str, Any]:
        """Generate a compliance report for all resources."""
        report = {
            "timestamp": None,
            "resources": {
                "clusters": [],
                "warehouses": [],
                "jobs": [],
                "pipelines": [],
                "experiments": [],
                "models": [],
                "catalogs": [],
                "schemas": [],
                "tables": [],
                "volumes": [],
                "repos": [],
                "serving_endpoints": []
            },
            "summary": {
                "total_resources": 0,
                "tagged_resources": 0,
                "compliance_rate": 0.0,
                "required_tags": ["Environment", "Owner", "Project"]
            }
        }
        
        try:
            import datetime
            report["timestamp"] = datetime.datetime.now().isoformat()
            
            # Required tags for compliance
            required_tags = report["summary"]["required_tags"]
            
            # Check clusters
            clusters = self.client.list_clusters()
            for cluster in clusters:
                tags = self.get_cluster_tags(cluster.cluster_id)
                has_required = all(tag in tags for tag in required_tags)
                
                report["resources"]["clusters"].append({
                    "id": cluster.cluster_id,
                    "name": cluster.cluster_name,
                    "tags": tags,
                    "compliant": has_required,
                    "missing_tags": [tag for tag in required_tags if tag not in tags]
                })
            
            # Check warehouses
            warehouses = self.client.list_warehouses()
            for warehouse in warehouses:
                tags = self.get_warehouse_tags(warehouse.id)
                has_required = all(tag in tags for tag in required_tags)
                
                report["resources"]["warehouses"].append({
                    "id": warehouse.id,
                    "name": warehouse.name,
                    "tags": tags,
                    "compliant": has_required,
                    "missing_tags": [tag for tag in required_tags if tag not in tags]
                })
            
            # Check jobs
            jobs = self.client.list_jobs()
            for job in jobs:
                tags = self.get_job_tags(job.job_id)
                has_required = all(tag in tags for tag in required_tags)
                
                report["resources"]["jobs"].append({
                    "id": job.job_id,
                    "name": job.settings.name if job.settings else "Unknown",
                    "tags": tags,
                    "compliant": has_required,
                    "missing_tags": [tag for tag in required_tags if tag not in tags]
                })
            
            # Check pipelines
            pipelines = self.client.list_pipelines()
            for pipeline in pipelines:
                tags = self.get_pipeline_tags(pipeline.pipeline_id)
                has_required = all(tag in tags for tag in required_tags)
                
                report["resources"]["pipelines"].append({
                    "id": pipeline.pipeline_id,
                    "name": pipeline.name,
                    "tags": tags,
                    "compliant": has_required,
                    "missing_tags": [tag for tag in required_tags if tag not in tags]
                })
            
            # Additional resource types would be added here similarly...
            
            # Calculate summary statistics
            all_resources = []
            for resource_type in report["resources"]:
                all_resources.extend(report["resources"][resource_type])
            
            report["summary"]["total_resources"] = len(all_resources)
            report["summary"]["tagged_resources"] = sum(1 for r in all_resources if r["compliant"])
            
            if report["summary"]["total_resources"] > 0:
                report["summary"]["compliance_rate"] = report["summary"]["tagged_resources"] / report["summary"]["total_resources"]
            
        except Exception as e:
            logger.error(f"Error generating compliance report: {e}")
            report["error"] = str(e)
        
        return report