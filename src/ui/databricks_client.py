"""
Databricks SQL Client for Databricks IQ
Handles connections and queries to Databricks SQL warehouses
"""

import streamlit as st
import pandas as pd
import logging
import os
from typing import Optional, Dict, Any
from databricks import sql
from databricks.sdk.core import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_warehouse_http_path() -> Optional[str]:
    """
    Get the SQL warehouse HTTP path from environment variable
    
    Returns:
        HTTP path string or None if environment variable not set
    """
    warehouse_id = os.getenv('SQL_WAREHOUSE')
    if warehouse_id:
        return f"/sql/1.0/warehouses/{warehouse_id}"
    return None

class DatabricksClient:
    """Client for managing Databricks SQL connections and queries"""
    
    def __init__(self, catalog: str = "databricksiq", schema: str = "main"):
        """
        Initialize the Databricks client
        
        Args:
            catalog: The catalog name to query from
            schema: The schema name to query from
        """
        self.catalog = catalog
        self.schema = schema
        self.config = Config()  # Uses environment variables
        self._connection = None
        
    @st.cache_resource
    def get_connection(_self, http_path: Optional[str] = None):
        """
        Get a cached connection to Databricks SQL warehouse
        
        Args:
            http_path: The HTTP path to the SQL warehouse (optional, uses env var if not provided)
            
        Returns:
            Databricks SQL connection object
        """
        if http_path is None:
            http_path = get_warehouse_http_path()
            
        if not http_path:
            logger.error("No SQL warehouse configured. Set SQL_WAREHOUSE environment variable.")
            st.error("No SQL warehouse configured. Set SQL_WAREHOUSE environment variable.")
            return None
            
        try:
            logger.info(f"Establishing connection to Databricks SQL warehouse: {http_path}")
            return sql.connect(
                server_hostname=_self.config.host,
                http_path=http_path,
                credentials_provider=lambda: _self.config.authenticate,
            )
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {str(e)}")
            st.error(f"Failed to connect to Databricks: {str(e)}")
            return None
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def query_table(_self, table_name: str, http_path: Optional[str] = None, limit: int = 1000, 
                   filters: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """
        Query a table from Databricks SQL
        
        Args:
            table_name: Name of the table to query
            http_path: HTTP path to the SQL warehouse (optional, uses env var if not provided)
            limit: Maximum number of rows to return
            filters: Optional filters to apply to the query
            
        Returns:
            pandas DataFrame with query results or None if failed
        """
        if http_path is None:
            http_path = get_warehouse_http_path()
            
        conn = _self.get_connection(http_path)
        if not conn:
            return None
            
        try:
            # Build the base query
            query = f"SELECT * FROM {_self.catalog}.{_self.schema}.{table_name}"
            
            # Add filters if provided
            where_clauses = []
            if filters:
                for column, value in filters.items():
                    if value is not None:
                        if isinstance(value, str):
                            where_clauses.append(f"{column} = '{value}'")
                        elif isinstance(value, (list, tuple)):
                            # Handle IN clauses
                            if len(value) > 0:
                                if isinstance(value[0], str):
                                    value_str = "', '".join(value)
                                    where_clauses.append(f"{column} IN ('{value_str}')")
                                else:
                                    value_str = ", ".join(map(str, value))
                                    where_clauses.append(f"{column} IN ({value_str})")
                        else:
                            where_clauses.append(f"{column} = {value}")
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            # Add limit
            query += f" LIMIT {limit}"
            
            logger.info(f"Executing query: {query}")
            
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall_arrow()
                df = result.to_pandas()
                
                logger.info(f"Query returned {len(df)} rows")
                return df
                
        except Exception as e:
            logger.error(f"Query failed for table {table_name}: {str(e)}")
            st.error(f"Query failed for table {table_name}: {str(e)}")
            return None
    
    @st.cache_data(ttl=300)
    def get_table_schema(_self, table_name: str, http_path: Optional[str] = None) -> Optional[Dict[str, str]]:
        """
        Get the schema (column names and types) for a table
        
        Args:
            table_name: Name of the table
            http_path: HTTP path to the SQL warehouse (optional, uses env var if not provided)
            
        Returns:
            Dictionary mapping column names to their types
        """
        if http_path is None:
            http_path = get_warehouse_http_path()
            
        conn = _self.get_connection(http_path)
        if not conn:
            return None
            
        try:
            query = f"DESCRIBE {_self.catalog}.{_self.schema}.{table_name}"
            
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall_arrow()
                df = result.to_pandas()
                
                # Convert to dictionary
                schema = {}
                for _, row in df.iterrows():
                    schema[row['col_name']] = row['data_type']
                
                return schema
                
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            return None
    
    @st.cache_data(ttl=600)  # Cache for 10 minutes
    def list_tables(_self, http_path: Optional[str] = None) -> Optional[list]:
        """
        List all tables in the configured catalog and schema
        
        Args:
            http_path: HTTP path to the SQL warehouse (optional, uses env var if not provided)
            
        Returns:
            List of table names
        """
        if http_path is None:
            http_path = get_warehouse_http_path()
            
        conn = _self.get_connection(http_path)
        if not conn:
            return None
            
        try:
            query = f"SHOW TABLES IN {_self.catalog}.{_self.schema}"
            
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall_arrow()
                df = result.to_pandas()
                
                # Extract table names
                if 'tableName' in df.columns:
                    return df['tableName'].tolist()
                elif 'table_name' in df.columns:
                    return df['table_name'].tolist()
                else:
                    # Try to find the table name column
                    for col in df.columns:
                        if 'table' in col.lower():
                            return df[col].tolist()
                    
                    logger.warning("Could not identify table name column")
                    return []
                
        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
            return None
    
    def test_connection(self, http_path: Optional[str] = None) -> bool:
        """
        Test the connection to Databricks SQL warehouse
        
        Args:
            http_path: HTTP path to the SQL warehouse (optional, uses env var if not provided)
            
        Returns:
            True if connection successful, False otherwise
        """
        if http_path is None:
            http_path = get_warehouse_http_path()
            
        conn = self.get_connection(http_path)
        if not conn:
            return False
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def get_warehouse_info(self, http_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get information about the SQL warehouse
        
        Args:
            http_path: HTTP path to the SQL warehouse (optional, uses env var if not provided)
            
        Returns:
            Dictionary with warehouse information
        """
        if http_path is None:
            http_path = get_warehouse_http_path()
            
        try:
            # Extract warehouse ID from http_path
            warehouse_id = http_path.split("/")[-1] if "/" in http_path else http_path
            
            return {
                "warehouse_id": warehouse_id,
                "http_path": http_path,
                "catalog": self.catalog,
                "schema": self.schema,
                "host": self.config.host if hasattr(self.config, 'host') else "N/A"
            }
        except Exception as e:
            logger.error(f"Failed to get warehouse info: {str(e)}")
            return None

# Global client instance
_databricks_client = None

def get_databricks_client(catalog: str = "databricksiq", schema: str = "main") -> DatabricksClient:
    """
    Get a singleton instance of the Databricks client
    
    Args:
        catalog: The catalog name
        schema: The schema name
        
    Returns:
        DatabricksClient instance
    """
    global _databricks_client
    if _databricks_client is None or _databricks_client.catalog != catalog or _databricks_client.schema != schema:
        _databricks_client = DatabricksClient(catalog, schema)
    return _databricks_client