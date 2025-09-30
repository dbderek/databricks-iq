"""Budget policy management for Databricks resources."""

import logging
from typing import Any, Dict, List, Optional
from ..clients.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class BudgetManager:
    """Manages budget policies and budgets for Databricks resources."""
    
    def __init__(self, client: DatabricksClient):
        """Initialize BudgetManager with Databricks client."""
        self.client = client
        if not client or not client.account_client:
            logger.warning("Budget manager requires account client - some features may not be available")
    
    def create_budget_policy(self, name: str, display_name: str, max_monthly_budget: float, 
                           alert_thresholds: Optional[List[float]] = None) -> Optional[str]:
        """Create a new budget policy."""
        try:
            if not self.client.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            
            # Default alert thresholds if not provided
            if alert_thresholds is None:
                alert_thresholds = [0.5, 0.75, 0.9]  # 50%, 75%, 90%
            
            policy_data = {
                "name": name,
                "display_name": display_name,
                "max_monthly_budget_per_user": max_monthly_budget,
                "alert_on_budget_percentage": alert_thresholds
            }
            
            result = self.client.create_budget_policy(**policy_data)
            logger.info(f"Created budget policy: {name} (ID: {result.policy_id})")
            return result.policy_id
            
        except Exception as e:
            logger.error(f"Error creating budget policy: {e}")
            return None
    
    def get_budget_policy(self, policy_id: str) -> Optional[Dict[str, Any]]:
        """Get budget policy details."""
        try:
            policy = self.client.get_budget_policy(policy_id)
            return {
                "policy_id": policy.policy_id,
                "name": policy.name,
                "display_name": policy.display_name,
                "max_monthly_budget": policy.max_monthly_budget_per_user,
                "alert_thresholds": policy.alert_on_budget_percentage,
                "created_time": getattr(policy, 'created_time', None),
                "updated_time": getattr(policy, 'updated_time', None)
            }
        except Exception as e:
            logger.error(f"Error getting budget policy {policy_id}: {e}")
            return None
    
    def list_budget_policies(self) -> List[Dict[str, Any]]:
        """List all budget policies."""
        try:
            policies = self.client.list_budget_policies()
            policy_list = []
            
            for policy in policies:
                policy_list.append({
                    "policy_id": policy.policy_id,
                    "name": policy.name,
                    "display_name": policy.display_name,
                    "max_monthly_budget": policy.max_monthly_budget_per_user,
                    "alert_thresholds": policy.alert_on_budget_percentage,
                    "created_time": getattr(policy, 'created_time', None),
                    "updated_time": getattr(policy, 'updated_time', None)
                })
            
            return policy_list
            
        except Exception as e:
            logger.error(f"Error listing budget policies: {e}")
            return []
    
    def update_budget_policy(self, policy_id: str, **kwargs) -> bool:
        """Update an existing budget policy."""
        try:
            self.client.update_budget_policy(policy_id, **kwargs)
            logger.info(f"Updated budget policy: {policy_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating budget policy {policy_id}: {e}")
            return False
    
    def delete_budget_policy(self, policy_id: str) -> bool:
        """Delete a budget policy."""
        try:
            self.client.delete_budget_policy(policy_id)
            logger.info(f"Deleted budget policy: {policy_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting budget policy {policy_id}: {e}")
            return False
    
    def create_budget(self, name: str, display_name: str, budget_configuration_id: str, 
                     alert_emails: List[str], budget_policy_id: Optional[str] = None) -> Optional[str]:
        """Create a new budget."""
        try:
            if not self.client.account_client:
                raise ValueError("Account client not available - requires DATABRICKS_ACCOUNT_ID")
            
            budget_data = {
                "name": name,
                "display_name": display_name,
                "budget_configuration_id": budget_configuration_id,
                "alert_configurations": [
                    {
                        "action_configurations": [
                            {
                                "action_type": "EMAIL",
                                "target": email
                            }
                            for email in alert_emails
                        ]
                    }
                ]
            }
            
            if budget_policy_id:
                budget_data["budget_policy_id"] = budget_policy_id
            
            result = self.client.create_budget(**budget_data)
            logger.info(f"Created budget: {name} (ID: {result.budget_id})")
            return result.budget_id
            
        except Exception as e:
            logger.error(f"Error creating budget: {e}")
            return None
    
    def get_budget(self, budget_id: str) -> Optional[Dict[str, Any]]:
        """Get budget details."""
        try:
            budget = self.client.get_budget(budget_id)
            return {
                "budget_id": budget.budget_id,
                "name": budget.name,
                "display_name": budget.display_name,
                "budget_configuration_id": budget.budget_configuration_id,
                "budget_policy_id": getattr(budget, 'budget_policy_id', None),
                "alert_configurations": budget.alert_configurations,
                "created_time": getattr(budget, 'created_time', None),
                "updated_time": getattr(budget, 'updated_time', None)
            }
        except Exception as e:
            logger.error(f"Error getting budget {budget_id}: {e}")
            return None
    
    def list_budgets(self) -> List[Dict[str, Any]]:
        """List all budgets."""
        try:
            budgets = self.client.list_budgets()
            budget_list = []
            
            for budget in budgets:
                budget_list.append({
                    "budget_id": budget.budget_id,
                    "name": budget.name,
                    "display_name": budget.display_name,
                    "budget_configuration_id": budget.budget_configuration_id,
                    "budget_policy_id": getattr(budget, 'budget_policy_id', None),
                    "alert_configurations": budget.alert_configurations,
                    "created_time": getattr(budget, 'created_time', None),
                    "updated_time": getattr(budget, 'updated_time', None)
                })
            
            return budget_list
            
        except Exception as e:
            logger.error(f"Error listing budgets: {e}")
            return []
    
    def update_budget(self, budget_id: str, **kwargs) -> bool:
        """Update an existing budget."""
        try:
            self.client.update_budget(budget_id, **kwargs)
            logger.info(f"Updated budget: {budget_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating budget {budget_id}: {e}")
            return False
    
    def delete_budget(self, budget_id: str) -> bool:
        """Delete a budget."""
        try:
            self.client.delete_budget(budget_id)
            logger.info(f"Deleted budget: {budget_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting budget {budget_id}: {e}")
            return False
    
    def apply_budget_policy_to_resource(self, resource_type: str, resource_id: str, 
                                       budget_policy_id: str) -> bool:
        """Apply a budget policy to a specific resource via tagging."""
        try:
            # Apply budget policy by setting a standardized tag
            from ..managers.tag_manager import TagManager
            tag_manager = TagManager(self.client)
            
            success = tag_manager.set_tag(
                resource_type, 
                resource_id, 
                "budget_policy_id", 
                budget_policy_id
            )
            
            if success:
                logger.info(f"Applied budget policy {budget_policy_id} to {resource_type} {resource_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error applying budget policy to {resource_type} {resource_id}: {e}")
            return False
    
    def get_resources_with_budget_policy(self, budget_policy_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get all resources that have a specific budget policy applied."""
        resources = {
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
        }
        
        try:
            from ..managers.tag_manager import TagManager
            tag_manager = TagManager(self.client)
            
            # Check clusters
            clusters = self.client.list_clusters()
            for cluster in clusters:
                tags = tag_manager.get_cluster_tags(cluster.cluster_id)
                if tags.get("budget_policy_id") == budget_policy_id:
                    resources["clusters"].append({
                        "id": cluster.cluster_id,
                        "name": cluster.cluster_name,
                        "tags": tags
                    })
            
            # Check warehouses
            warehouses = self.client.list_warehouses()
            for warehouse in warehouses:
                tags = tag_manager.get_warehouse_tags(warehouse.id)
                if tags.get("budget_policy_id") == budget_policy_id:
                    resources["warehouses"].append({
                        "id": warehouse.id,
                        "name": warehouse.name,
                        "tags": tags
                    })
            
            # Check jobs
            jobs = self.client.list_jobs()
            for job in jobs:
                tags = tag_manager.get_job_tags(job.job_id)
                if tags.get("budget_policy_id") == budget_policy_id:
                    resources["jobs"].append({
                        "id": job.job_id,
                        "name": job.settings.name if job.settings else "Unknown",
                        "tags": tags
                    })
            
            # Check pipelines
            pipelines = self.client.list_pipelines()
            for pipeline in pipelines:
                tags = tag_manager.get_pipeline_tags(pipeline.pipeline_id)
                if tags.get("budget_policy_id") == budget_policy_id:
                    resources["pipelines"].append({
                        "id": pipeline.pipeline_id,
                        "name": pipeline.name,
                        "tags": tags
                    })
            
            # Additional resource types would be checked here similarly...
            
        except Exception as e:
            logger.error(f"Error getting resources with budget policy {budget_policy_id}: {e}")
        
        return resources
    
    def get_budget_compliance_report(self) -> Dict[str, Any]:
        """Generate a report on budget policy compliance across resources."""
        report = {
            "timestamp": None,
            "policies": [],
            "resource_coverage": {
                "total_resources": 0,
                "resources_with_policy": 0,
                "coverage_rate": 0.0
            },
            "policy_usage": {}
        }
        
        try:
            import datetime
            report["timestamp"] = datetime.datetime.now().isoformat()
            
            # Get all budget policies
            policies = self.list_budget_policies()
            report["policies"] = policies
            
            from ..managers.tag_manager import TagManager
            tag_manager = TagManager(self.client)
            
            # Count total resources and those with budget policies
            total_resources = 0
            resources_with_policy = 0
            policy_usage = {}
            
            # Initialize policy usage counters
            for policy in policies:
                policy_usage[policy["policy_id"]] = {
                    "policy_name": policy["name"],
                    "resource_count": 0,
                    "resources": {
                        "clusters": [],
                        "warehouses": [],
                        "jobs": [],
                        "pipelines": []
                    }
                }
            
            # Check clusters
            clusters = self.client.list_clusters()
            for cluster in clusters:
                total_resources += 1
                tags = tag_manager.get_cluster_tags(cluster.cluster_id)
                budget_policy_id = tags.get("budget_policy_id")
                
                if budget_policy_id:
                    resources_with_policy += 1
                    if budget_policy_id in policy_usage:
                        policy_usage[budget_policy_id]["resource_count"] += 1
                        policy_usage[budget_policy_id]["resources"]["clusters"].append({
                            "id": cluster.cluster_id,
                            "name": cluster.cluster_name
                        })
            
            # Check warehouses
            warehouses = self.client.list_warehouses()
            for warehouse in warehouses:
                total_resources += 1
                tags = tag_manager.get_warehouse_tags(warehouse.id)
                budget_policy_id = tags.get("budget_policy_id")
                
                if budget_policy_id:
                    resources_with_policy += 1
                    if budget_policy_id in policy_usage:
                        policy_usage[budget_policy_id]["resource_count"] += 1
                        policy_usage[budget_policy_id]["resources"]["warehouses"].append({
                            "id": warehouse.id,
                            "name": warehouse.name
                        })
            
            # Check jobs
            jobs = self.client.list_jobs()
            for job in jobs:
                total_resources += 1
                tags = tag_manager.get_job_tags(job.job_id)
                budget_policy_id = tags.get("budget_policy_id")
                
                if budget_policy_id:
                    resources_with_policy += 1
                    if budget_policy_id in policy_usage:
                        policy_usage[budget_policy_id]["resource_count"] += 1
                        policy_usage[budget_policy_id]["resources"]["jobs"].append({
                            "id": job.job_id,
                            "name": job.settings.name if job.settings else "Unknown"
                        })
            
            # Check pipelines
            pipelines = self.client.list_pipelines()
            for pipeline in pipelines:
                total_resources += 1
                tags = tag_manager.get_pipeline_tags(pipeline.pipeline_id)
                budget_policy_id = tags.get("budget_policy_id")
                
                if budget_policy_id:
                    resources_with_policy += 1
                    if budget_policy_id in policy_usage:
                        policy_usage[budget_policy_id]["resource_count"] += 1
                        policy_usage[budget_policy_id]["resources"]["pipelines"].append({
                            "id": pipeline.pipeline_id,
                            "name": pipeline.name
                        })
            
            # Calculate summary statistics
            report["resource_coverage"]["total_resources"] = total_resources
            report["resource_coverage"]["resources_with_policy"] = resources_with_policy
            
            if total_resources > 0:
                report["resource_coverage"]["coverage_rate"] = resources_with_policy / total_resources
            
            report["policy_usage"] = policy_usage
            
        except Exception as e:
            logger.error(f"Error generating budget compliance report: {e}")
            report["error"] = str(e)
        
        return report