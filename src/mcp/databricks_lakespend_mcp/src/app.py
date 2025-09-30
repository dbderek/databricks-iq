#!/usr/bin/env python3
"""
Databricks Tags MCP Server

This server provides comprehensive tag management tools for Databricks resources
including clusters, SQL warehouses, jobs, and pipelines.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from mcp.server.fastmcp import FastMCP

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.sql import EndpointInfo
from databricks.sdk.service.jobs import Job
from databricks.sdk.service.pipelines import PipelineStateInfo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


class DatabricksClient:
    """Wrapper class for Databricks SDK operations."""
    
    def __init__(self):
        """Initialize the Databricks client with authentication."""
        try:
            self.client = WorkspaceClient()
            logger.info("Databricks client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Databricks client: {e}")
            self.client = None
    
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
            # Update cluster with new configuration
            self.client.clusters.edit(
                cluster_id=cluster_id,
                cluster_name=cluster.cluster_name,
                spark_version=cluster.spark_version,
                node_type_id=cluster.node_type_id,
                num_workers=cluster.num_workers,
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


class TagManager:
    """Manager class for Databricks resource tag operations."""
    
    def __init__(self, db_client: DatabricksClient):
        """Initialize the tag manager with a Databricks client."""
        self.db_client = db_client
    
    def _merge_tags(self, existing_tags: Dict[str, str], new_tags: Dict[str, str], operation: str) -> Dict[str, str]:
        """Merge tags based on the specified operation."""
        if operation == "replace":
            return new_tags.copy()
        elif operation == "remove":
            result = existing_tags.copy()
            for key in new_tags.keys():
                result.pop(key, None)
            return result
        else:  # merge (default)
            result = existing_tags.copy()
            result.update(new_tags)
            return result
    
    # Cluster tag management
    def get_cluster_tags(self, cluster_id: str) -> Dict[str, str]:
        """Get all tags for a specific cluster."""
        try:
            cluster = self.db_client.get_cluster(cluster_id)
            if not cluster:
                raise ValueError(f"Cluster {cluster_id} not found")
            
            return cluster.custom_tags or {}
        except Exception as e:
            logger.error(f"Error getting tags for cluster {cluster_id}: {e}")
            raise
    
    def update_cluster_tags(self, cluster_id: str, tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
        """Update tags on a cluster."""
        try:
            # Get current tags
            current_tags = self.get_cluster_tags(cluster_id)
            
            # Apply operation
            new_tags = self._merge_tags(current_tags, tags, operation)
            
            # Update cluster with new tags
            self.db_client.edit_cluster(cluster_id, custom_tags=new_tags)
            
            return {
                "success": True,
                "previous_tags": current_tags,
                "new_tags": new_tags,
                "operation": operation
            }
        except Exception as e:
            logger.error(f"Error updating tags for cluster {cluster_id}: {e}")
            raise
    
    def get_all_clusters_with_tags(self) -> List[Dict[str, Any]]:
        """Get all clusters and their current tags."""
        try:
            clusters = self.db_client.list_clusters()
            result = []
            
            for cluster in clusters:
                result.append({
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "state": cluster.state.value if cluster.state else "unknown",
                    "tags": cluster.custom_tags or {},
                    "tag_count": len(cluster.custom_tags or {})
                })
            
            return result
        except Exception as e:
            logger.error(f"Error getting all clusters with tags: {e}")
            raise
    
    # SQL Warehouse tag management
    def get_warehouse_tags(self, warehouse_id: str) -> Dict[str, str]:
        """Get all tags for a specific SQL warehouse."""
        try:
            warehouse = self.db_client.get_warehouse(warehouse_id)
            if not warehouse:
                raise ValueError(f"Warehouse {warehouse_id} not found")
            
            return warehouse.tags or {}
        except Exception as e:
            logger.error(f"Error getting tags for warehouse {warehouse_id}: {e}")
            raise
    
    def update_warehouse_tags(self, warehouse_id: str, tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
        """Update tags on a SQL warehouse."""
        try:
            # Get current tags
            current_tags = self.get_warehouse_tags(warehouse_id)
            
            # Apply operation
            new_tags = self._merge_tags(current_tags, tags, operation)
            
            # Update warehouse with new tags
            self.db_client.edit_warehouse(warehouse_id, tags=new_tags)
            
            return {
                "success": True,
                "previous_tags": current_tags,
                "new_tags": new_tags,
                "operation": operation
            }
        except Exception as e:
            logger.error(f"Error updating tags for warehouse {warehouse_id}: {e}")
            raise
    
    def get_all_warehouses_with_tags(self) -> List[Dict[str, Any]]:
        """Get all SQL warehouses and their current tags."""
        try:
            warehouses = self.db_client.list_warehouses()
            result = []
            
            for warehouse in warehouses:
                result.append({
                    "warehouse_id": warehouse.id,
                    "warehouse_name": warehouse.name,
                    "state": warehouse.state.value if warehouse.state else "unknown",
                    "cluster_size": warehouse.cluster_size,
                    "tags": warehouse.tags or {},
                    "tag_count": len(warehouse.tags or {})
                })
            
            return result
        except Exception as e:
            logger.error(f"Error getting all warehouses with tags: {e}")
            raise
    
    # Job tag management
    def get_job_tags(self, job_id: int) -> Dict[str, str]:
        """Get all tags for a specific job."""
        try:
            job = self.db_client.get_job(job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")
            
            return job.settings.tags or {} if job.settings else {}
        except Exception as e:
            logger.error(f"Error getting tags for job {job_id}: {e}")
            raise
    
    def update_job_tags(self, job_id: int, tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
        """Update tags on a job."""
        try:
            # Get current job settings
            job = self.db_client.get_job(job_id)
            if not job or not job.settings:
                raise ValueError(f"Job {job_id} not found or has no settings")
            
            current_tags = job.settings.tags or {}
            
            # Apply operation
            new_tags = self._merge_tags(current_tags, tags, operation)
            
            # Update job with new tags
            new_settings = job.settings
            new_settings.tags = new_tags
            
            self.db_client.update_job(job_id, new_settings=new_settings)
            
            return {
                "success": True,
                "previous_tags": current_tags,
                "new_tags": new_tags,
                "operation": operation
            }
        except Exception as e:
            logger.error(f"Error updating tags for job {job_id}: {e}")
            raise
    
    def get_all_jobs_with_tags(self) -> List[Dict[str, Any]]:
        """Get all jobs and their current tags."""
        try:
            jobs = self.db_client.list_jobs()
            result = []
            
            for job in jobs:
                tags = job.settings.tags or {} if job.settings else {}
                result.append({
                    "job_id": job.job_id,
                    "job_name": job.settings.name if job.settings else "Unknown",
                    "creator_user_name": job.creator_user_name,
                    "tags": tags,
                    "tag_count": len(tags)
                })
            
            return result
        except Exception as e:
            logger.error(f"Error getting all jobs with tags: {e}")
            raise
    
    # Pipeline tag management
    def get_pipeline_tags(self, pipeline_id: str) -> Dict[str, str]:
        """Get all tags for a specific pipeline."""
        try:
            pipeline = self.db_client.get_pipeline(pipeline_id)
            if not pipeline:
                raise ValueError(f"Pipeline {pipeline_id} not found")
            
            # Pipeline tags are typically in the spec
            return pipeline.spec.configuration or {} if pipeline.spec else {}
        except Exception as e:
            logger.error(f"Error getting tags for pipeline {pipeline_id}: {e}")
            raise
    
    def update_pipeline_tags(self, pipeline_id: str, tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
        """Update tags on a pipeline."""
        try:
            # Get current pipeline
            pipeline = self.db_client.get_pipeline(pipeline_id)
            if not pipeline or not pipeline.spec:
                raise ValueError(f"Pipeline {pipeline_id} not found or has no spec")
            
            current_tags = pipeline.spec.configuration or {}
            
            # Apply operation
            new_tags = self._merge_tags(current_tags, tags, operation)
            
            # Update pipeline with new configuration
            self.db_client.update_pipeline(pipeline_id, configuration=new_tags)
            
            return {
                "success": True,
                "previous_tags": current_tags,
                "new_tags": new_tags,
                "operation": operation
            }
        except Exception as e:
            logger.error(f"Error updating tags for pipeline {pipeline_id}: {e}")
            raise
    
    def get_all_pipelines_with_tags(self) -> List[Dict[str, Any]]:
        """Get all pipelines and their current tags."""
        try:
            pipelines = self.db_client.list_pipelines()
            result = []
            
            for pipeline in pipelines:
                tags = pipeline.spec.configuration or {} if pipeline.spec else {}
                result.append({
                    "pipeline_id": pipeline.pipeline_id,
                    "pipeline_name": pipeline.spec.name if pipeline.spec else "Unknown",
                    "state": pipeline.state.value if pipeline.state else "unknown",
                    "tags": tags,
                    "tag_count": len(tags)
                })
            
            return result
        except Exception as e:
            logger.error(f"Error getting all pipelines with tags: {e}")
            raise
    
    # Bulk operations
    def bulk_update_tags(self, resources: List[Dict[str, Union[str, int]]], tags: Dict[str, str], operation: str = "merge") -> List[Dict[str, Any]]:
        """Update tags across multiple resources at once."""
        results = []
        
        for resource in resources:
            resource_type = resource.get("type")
            resource_id = resource.get("id")
            
            try:
                if resource_type == "cluster":
                    result = self.update_cluster_tags(str(resource_id), tags, operation)
                elif resource_type == "warehouse":
                    result = self.update_warehouse_tags(str(resource_id), tags, operation)
                elif resource_type == "job":
                    result = self.update_job_tags(int(resource_id), tags, operation)
                elif resource_type == "pipeline":
                    result = self.update_pipeline_tags(str(resource_id), tags, operation)
                else:
                    result = {"success": False, "error": f"Unknown resource type: {resource_type}"}
                
                results.append({
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "result": result
                })
                
            except Exception as e:
                logger.error(f"Error updating tags for {resource_type} {resource_id}: {e}")
                results.append({
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "result": {"success": False, "error": str(e)}
                })
        
        return results
    
    def find_resources_by_tag(self, tag_key: str, tag_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """Find all resources that have specific tag keys or values."""
        matching_resources = []
        
        try:
            # Search clusters
            clusters = self.get_all_clusters_with_tags()
            for cluster in clusters:
                tags = cluster.get("tags", {})
                if tag_key in tags:
                    if tag_value is None or tags[tag_key] == tag_value:
                        matching_resources.append({
                            "resource_type": "cluster",
                            "resource_id": cluster["cluster_id"],
                            "resource_name": cluster["cluster_name"],
                            "tag_value": tags[tag_key],
                            "all_tags": tags
                        })
            
            # Search warehouses
            warehouses = self.get_all_warehouses_with_tags()
            for warehouse in warehouses:
                tags = warehouse.get("tags", {})
                if tag_key in tags:
                    if tag_value is None or tags[tag_key] == tag_value:
                        matching_resources.append({
                            "resource_type": "warehouse",
                            "resource_id": warehouse["warehouse_id"],
                            "resource_name": warehouse["warehouse_name"],
                            "tag_value": tags[tag_key],
                            "all_tags": tags
                        })
            
            # Search jobs
            jobs = self.get_all_jobs_with_tags()
            for job in jobs:
                tags = job.get("tags", {})
                if tag_key in tags:
                    if tag_value is None or tags[tag_key] == tag_value:
                        matching_resources.append({
                            "resource_type": "job",
                            "resource_id": job["job_id"],
                            "resource_name": job["job_name"],
                            "tag_value": tags[tag_key],
                            "all_tags": tags
                        })
            
            # Search pipelines
            pipelines = self.get_all_pipelines_with_tags()
            for pipeline in pipelines:
                tags = pipeline.get("tags", {})
                if tag_key in tags:
                    if tag_value is None or tags[tag_key] == tag_value:
                        matching_resources.append({
                            "resource_type": "pipeline",
                            "resource_id": pipeline["pipeline_id"],
                            "resource_name": pipeline["pipeline_name"],
                            "tag_value": tags[tag_key],
                            "all_tags": tags
                        })
            
            return matching_resources
            
        except Exception as e:
            logger.error(f"Error finding resources by tag: {e}")
            raise
    
    def generate_compliance_report(self, required_tags: List[str]) -> Dict[str, Any]:
        """Generate a report on tag compliance across resources."""
        try:
            report = {
                "summary": {
                    "total_resources": 0,
                    "compliant_resources": 0,
                    "non_compliant_resources": 0,
                    "compliance_percentage": 0.0
                },
                "by_resource_type": {},
                "required_tags": required_tags,
                "non_compliant_details": []
            }
            
            resource_types = [
                ("cluster", self.get_all_clusters_with_tags()),
                ("warehouse", self.get_all_warehouses_with_tags()),
                ("job", self.get_all_jobs_with_tags()),
                ("pipeline", self.get_all_pipelines_with_tags())
            ]
            
            for resource_type, resources in resource_types:
                type_report = {
                    "total": len(resources),
                    "compliant": 0,
                    "non_compliant": 0,
                    "compliance_percentage": 0.0
                }
                
                for resource in resources:
                    tags = resource.get("tags", {})
                    missing_tags = [tag for tag in required_tags if tag not in tags]
                    
                    if not missing_tags:
                        type_report["compliant"] += 1
                    else:
                        type_report["non_compliant"] += 1
                        report["non_compliant_details"].append({
                            "resource_type": resource_type,
                            "resource_id": resource.get(f"{resource_type}_id"),
                            "resource_name": resource.get(f"{resource_type}_name", "Unknown"),
                            "missing_tags": missing_tags,
                            "current_tags": tags
                        })
                
                if type_report["total"] > 0:
                    type_report["compliance_percentage"] = (type_report["compliant"] / type_report["total"]) * 100
                
                report["by_resource_type"][resource_type] = type_report
                report["summary"]["total_resources"] += type_report["total"]
                report["summary"]["compliant_resources"] += type_report["compliant"]
                report["summary"]["non_compliant_resources"] += type_report["non_compliant"]
            
            if report["summary"]["total_resources"] > 0:
                report["summary"]["compliance_percentage"] = (
                    report["summary"]["compliant_resources"] / report["summary"]["total_resources"]
                ) * 100
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating compliance report: {e}")
            raise


# Initialize the MCP server
mcp = FastMCP("Databricks Tags MCP Server")

# Initialize Databricks client and tag manager
db_client = DatabricksClient()
tag_manager = TagManager(db_client)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

# Initialize the MCP server
mcp = FastMCP("Databricks Tags MCP Server")

# Initialize Databricks client and tag manager
db_client = DatabricksClient()
tag_manager = TagManager(db_client)


@mcp.tool()
def get_server_status() -> Dict[str, Any]:
    """Get the current status of the Databricks Tags MCP server."""
    try:
        # Test Databricks connection
        connection_status = db_client.test_connection()
        
        return {
            "server": "Databricks Tags MCP Server",
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat(),
            "databricks_connection": connection_status,
            "available_tools": [
                "list_cluster_tags",
                "update_cluster_tags", 
                "get_all_clusters_with_tags",
                "list_warehouse_tags",
                "update_warehouse_tags",
                "get_all_warehouses_with_tags",
                "list_job_tags",
                "update_job_tags",
                "get_all_jobs_with_tags",
                "list_pipeline_tags",
                "update_pipeline_tags",
                "get_all_pipelines_with_tags",
                "bulk_update_tags",
                "find_resources_by_tag",
                "tag_compliance_report"
            ]
        }
    except Exception as e:
        logger.error(f"Error getting server status: {e}")
        return {
            "server": "Databricks Tags MCP Server",
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "databricks_connection": False
        }


# Cluster Tag Management Tools
@mcp.tool()
def list_cluster_tags(cluster_id: str) -> Dict[str, Any]:
    """List all tags for a specific cluster.
    
    Args:
        cluster_id: The ID of the cluster to get tags for
    """
    try:
        tags = tag_manager.get_cluster_tags(cluster_id)
        return {
            "cluster_id": cluster_id,
            "tags": tags,
            "tag_count": len(tags),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error listing cluster tags for {cluster_id}: {e}")
        return {
            "cluster_id": cluster_id,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def update_cluster_tags(cluster_id: str, tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
    """Update tags on a cluster.
    
    Args:
        cluster_id: The ID of the cluster to update tags for
        tags: Dictionary of tag key-value pairs to apply
        operation: Operation type - "merge" (default), "replace", or "remove"
    """
    try:
        result = tag_manager.update_cluster_tags(cluster_id, tags, operation)
        return {
            "cluster_id": cluster_id,
            "operation": operation,
            "tags_applied": tags,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error updating cluster tags for {cluster_id}: {e}")
        return {
            "cluster_id": cluster_id,
            "operation": operation,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def get_all_clusters_with_tags() -> Dict[str, Any]:
    """Get all clusters and their current tags."""
    try:
        clusters = tag_manager.get_all_clusters_with_tags()
        return {
            "clusters": clusters,
            "cluster_count": len(clusters),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting all clusters with tags: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# SQL Warehouse Tag Management Tools
@mcp.tool()
def list_warehouse_tags(warehouse_id: str) -> Dict[str, Any]:
    """List all tags for a specific SQL warehouse.
    
    Args:
        warehouse_id: The ID of the SQL warehouse to get tags for
    """
    try:
        tags = tag_manager.get_warehouse_tags(warehouse_id)
        return {
            "warehouse_id": warehouse_id,
            "tags": tags,
            "tag_count": len(tags),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error listing warehouse tags for {warehouse_id}: {e}")
        return {
            "warehouse_id": warehouse_id,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def update_warehouse_tags(warehouse_id: str, tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
    """Update tags on a SQL warehouse.
    
    Args:
        warehouse_id: The ID of the SQL warehouse to update tags for
        tags: Dictionary of tag key-value pairs to apply
        operation: Operation type - "merge" (default), "replace", or "remove"
    """
    try:
        result = tag_manager.update_warehouse_tags(warehouse_id, tags, operation)
        return {
            "warehouse_id": warehouse_id,
            "operation": operation,
            "tags_applied": tags,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error updating warehouse tags for {warehouse_id}: {e}")
        return {
            "warehouse_id": warehouse_id,
            "operation": operation,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def get_all_warehouses_with_tags() -> Dict[str, Any]:
    """Get all SQL warehouses and their current tags."""
    try:
        warehouses = tag_manager.get_all_warehouses_with_tags()
        return {
            "warehouses": warehouses,
            "warehouse_count": len(warehouses),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting all warehouses with tags: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# Job Tag Management Tools
@mcp.tool()
def list_job_tags(job_id: int) -> Dict[str, Any]:
    """List all tags for a specific job.
    
    Args:
        job_id: The ID of the job to get tags for
    """
    try:
        tags = tag_manager.get_job_tags(job_id)
        return {
            "job_id": job_id,
            "tags": tags,
            "tag_count": len(tags),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error listing job tags for {job_id}: {e}")
        return {
            "job_id": job_id,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def update_job_tags(job_id: int, tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
    """Update tags on a job.
    
    Args:
        job_id: The ID of the job to update tags for
        tags: Dictionary of tag key-value pairs to apply
        operation: Operation type - "merge" (default), "replace", or "remove"
    """
    try:
        result = tag_manager.update_job_tags(job_id, tags, operation)
        return {
            "job_id": job_id,
            "operation": operation,
            "tags_applied": tags,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error updating job tags for {job_id}: {e}")
        return {
            "job_id": job_id,
            "operation": operation,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def get_all_jobs_with_tags() -> Dict[str, Any]:
    """Get all jobs and their current tags."""
    try:
        jobs = tag_manager.get_all_jobs_with_tags()
        return {
            "jobs": jobs,
            "job_count": len(jobs),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting all jobs with tags: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# Pipeline Tag Management Tools
@mcp.tool()
def list_pipeline_tags(pipeline_id: str) -> Dict[str, Any]:
    """List all tags for a specific pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline to get tags for
    """
    try:
        tags = tag_manager.get_pipeline_tags(pipeline_id)
        return {
            "pipeline_id": pipeline_id,
            "tags": tags,
            "tag_count": len(tags),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error listing pipeline tags for {pipeline_id}: {e}")
        return {
            "pipeline_id": pipeline_id,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def update_pipeline_tags(pipeline_id: str, tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
    """Update tags on a pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline to update tags for
        tags: Dictionary of tag key-value pairs to apply
        operation: Operation type - "merge" (default), "replace", or "remove"
    """
    try:
        result = tag_manager.update_pipeline_tags(pipeline_id, tags, operation)
        return {
            "pipeline_id": pipeline_id,
            "operation": operation,
            "tags_applied": tags,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error updating pipeline tags for {pipeline_id}: {e}")
        return {
            "pipeline_id": pipeline_id,
            "operation": operation,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def get_all_pipelines_with_tags() -> Dict[str, Any]:
    """Get all pipelines and their current tags."""
    try:
        pipelines = tag_manager.get_all_pipelines_with_tags()
        return {
            "pipelines": pipelines,
            "pipeline_count": len(pipelines),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting all pipelines with tags: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# Bulk Operations
@mcp.tool()
def bulk_update_tags(resources: List[Dict[str, Union[str, int]]], tags: Dict[str, str], operation: str = "merge") -> Dict[str, Any]:
    """Update tags across multiple resources at once.
    
    Args:
        resources: List of resource dictionaries with 'type' and 'id' keys
        tags: Dictionary of tag key-value pairs to apply
        operation: Operation type - "merge" (default), "replace", or "remove"
    """
    try:
        results = tag_manager.bulk_update_tags(resources, tags, operation)
        return {
            "operation": operation,
            "tags_applied": tags,
            "resources_processed": len(resources),
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in bulk update tags: {e}")
        return {
            "operation": operation,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def find_resources_by_tag(tag_key: str, tag_value: Optional[str] = None) -> Dict[str, Any]:
    """Find all resources that have specific tag keys or values.
    
    Args:
        tag_key: The tag key to search for
        tag_value: Optional tag value to match (if not provided, finds any value for the key)
    """
    try:
        resources = tag_manager.find_resources_by_tag(tag_key, tag_value)
        return {
            "search_criteria": {
                "tag_key": tag_key,
                "tag_value": tag_value
            },
            "resources": resources,
            "resource_count": len(resources),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error finding resources by tag: {e}")
        return {
            "search_criteria": {
                "tag_key": tag_key,
                "tag_value": tag_value
            },
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@mcp.tool()
def tag_compliance_report(required_tags: List[str]) -> Dict[str, Any]:
    """Generate a report on tag compliance across resources.
    
    Args:
        required_tags: List of tag keys that are required for compliance
    """
    try:
        report = tag_manager.generate_compliance_report(required_tags)
        return {
            "required_tags": required_tags,
            "compliance_report": report,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error generating compliance report: {e}")
        return {
            "required_tags": required_tags,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# Resources
@mcp.resource("server://info")
def get_server_info() -> str:
    """Get information about this Databricks Tags MCP server."""
    info = {
        "name": "Databricks Tags MCP Server",
        "version": "1.0.0",
        "description": "A comprehensive MCP server for managing Databricks resource tags",
        "supported_resources": [
            "clusters",
            "sql_warehouses", 
            "jobs",
            "pipelines"
        ],
        "tools": [
            "get_server_status",
            "list_cluster_tags",
            "update_cluster_tags",
            "get_all_clusters_with_tags",
            "list_warehouse_tags", 
            "update_warehouse_tags",
            "get_all_warehouses_with_tags",
            "list_job_tags",
            "update_job_tags",
            "get_all_jobs_with_tags",
            "list_pipeline_tags",
            "update_pipeline_tags", 
            "get_all_pipelines_with_tags",
            "bulk_update_tags",
            "find_resources_by_tag",
            "tag_compliance_report"
        ]
    }
    return json.dumps(info, indent=2)


@mcp.resource("databricks://connection")
def get_connection_info() -> str:
    """Get Databricks connection information."""
    try:
        connection_info = db_client.get_connection_info()
        return json.dumps(connection_info, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


# Create the MCP app
mcp_app = mcp.streamable_http_app()

# Create the main FastAPI app
app = FastAPI(
    title="Databricks Tags MCP Server",
    description="A comprehensive MCP server for managing Databricks resource tags",
    version="1.0.0",
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    """Serve the main web interface."""
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        connection_status = db_client.test_connection()
        return {
            "status": "healthy" if connection_status else "unhealthy",
            "databricks_connection": connection_status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# Mount the MCP app
app.mount("/", mcp_app)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
