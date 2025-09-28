"""
Utilities for Databricks IQ application
"""

import streamlit as st
import pandas as pd
import altair as alt
import json
import warnings
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Suppress warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Configure Altair
alt.data_transformers.disable_max_rows()

# Databricks color palette
DATABRICKS_COLORS = ['#FF3621', '#00A1F1', '#7C4DFF', '#00D4AA', '#FF8A00', '#E91E63', '#9C27B0', '#673AB7']

def safe_nlargest(df: pd.DataFrame, n: int, column: str) -> pd.DataFrame:
    """
    Safely get n largest values, handling dtype issues
    """
    if df.empty or column not in df.columns:
        return df
    
    try:
        # Ensure the column is numeric
        if df[column].dtype == 'object':
            df[column] = pd.to_numeric(df[column], errors='coerce')
        
        return df.nlargest(n, column)
    except Exception:
        # If nlargest fails, sort manually
        try:
            return df.sort_values(column, ascending=False).head(n)
        except Exception:
            return df.head(n)

def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert numeric columns from object/string dtype to proper numeric types
    This is especially important when loading data from Databricks SQL
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Define columns that should be numeric based on common patterns
    numeric_patterns = [
        'cost', 'price', 'amount', 'total', 'sum', 'count', 'runs', 
        'duration', 'time', 'size', 'bytes', 'mb', 'gb', 'cpu', 'memory',
        'effective', 'spend', 'billing', 'usage', 'hours'
    ]
    
    for column in df.columns:
        # Skip if already numeric
        if pd.api.types.is_numeric_dtype(df[column]):
            continue
            
        # Check if column name suggests it should be numeric
        if any(pattern in column.lower() for pattern in numeric_patterns):
            try:
                # Try to convert to numeric, coercing errors to NaN
                df[column] = pd.to_numeric(df[column], errors='coerce')
            except Exception:
                # If conversion fails, leave as is
                continue
        
        # Also check for columns that look like they contain numeric data
        elif df[column].dtype == 'object':
            try:
                # Sample a few non-null values to see if they're numeric strings
                sample = df[column].dropna().head(5)
                if not sample.empty:
                    # Try converting sample to check if it's numeric data stored as strings
                    test_conversion = pd.to_numeric(sample, errors='coerce')
                    if test_conversion.notna().all():
                        # If all sample values convert successfully, convert the whole column
                        df[column] = pd.to_numeric(df[column], errors='coerce')
            except Exception:
                continue
    
    return df

def load_data(table_name: str, http_path: Optional[str] = None, 
              filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Load data from live Databricks SQL
    
    Args:
        table_name: Name of the SQL table
        http_path: HTTP path to Databricks SQL warehouse (optional, uses env var if not provided)
        filters: Optional filters to apply to the query
    
    Returns:
        pandas DataFrame with the data
    """
    df = load_live_data(table_name, http_path, filters)
    
    # Convert numeric columns to proper dtypes
    df = convert_numeric_columns(df)
    return df

def load_live_data(table_name: str, http_path: Optional[str] = None, filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Load data from live Databricks SQL
    
    Args:
        table_name: Name of the SQL table
        http_path: HTTP path to Databricks SQL warehouse (optional, uses env var if not provided)
        filters: Optional filters to apply
    
    Returns:
        pandas DataFrame with the data
    """
    try:
        from databricks_client import get_databricks_client
        from config import DATA_CONFIG
        
        # Get Databricks client
        client = get_databricks_client(
            catalog=DATA_CONFIG["databricks"]["catalog"],
            schema=DATA_CONFIG["databricks"]["schema"]
        )
        
        # Query the data directly using table name
        df = client.query_table(
            table_name=table_name,
            http_path=http_path,
            limit=DATA_CONFIG["databricks"]["max_rows"],
            filters=filters
        )
        
        if df is not None and not df.empty:
            st.success(f"✅ Loaded {len(df)} rows from Databricks SQL table: {table_name}")
            return df
        else:
            st.error(f"❌ No data returned from SQL table: {table_name}")
            return pd.DataFrame()
            
    except ImportError:
        st.error("❌ Databricks client not available")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error loading from Databricks SQL table {table_name}: {str(e)}")
        return pd.DataFrame()

def get_data_source_info(http_path: Optional[str] = None) -> Dict[str, str]:
    """
    Get information about the Databricks SQL data source
    
    Args:
        http_path: HTTP path if using live data (optional, uses env var if not provided)
    
    Returns:
        Dictionary with data source information
    """
    try:
        from databricks_client import get_databricks_client
        from config import DATA_CONFIG
        
        client = get_databricks_client(
            catalog=DATA_CONFIG["databricks"]["catalog"],
            schema=DATA_CONFIG["databricks"]["schema"]
        )
        
        warehouse_info = client.get_warehouse_info(http_path)
        if warehouse_info:
            return {
                "source": "Databricks SQL",
                "catalog": warehouse_info["catalog"],
                "schema": warehouse_info["schema"],
                "warehouse_id": warehouse_info["warehouse_id"],
                "host": warehouse_info.get("host", "N/A")
            }
    except Exception:
        pass
    
    return {
        "source": "Databricks SQL",
        "catalog": "N/A",
        "schema": "N/A",
        "status": "Configuration needed"
    }

def is_empty_string(value):
    """Safely check if a value is empty string, handling pandas Series/arrays"""
    try:
        # Handle pandas Series or array inputs
        if hasattr(value, '__iter__') and not isinstance(value, str):
            if len(value) > 0:
                value = value.iloc[0] if hasattr(value, 'iloc') else value[0]
            else:
                return True
        
        # Check for null or empty values
        if pd.isna(value):
            return True
        if value == '' or value is None:
            return True
        return False
    except (ValueError, TypeError, AttributeError):
        return True

def parse_tags(tags_str):
    """Parse custom tags JSON string into a readable format"""
    # Check for null or empty values using our safe function
    if is_empty_string(tags_str):
        return {}
    
    # Handle pandas Series or array inputs by taking the first element
    if hasattr(tags_str, '__iter__') and not isinstance(tags_str, str):
        if len(tags_str) > 0:
            tags_str = tags_str.iloc[0] if hasattr(tags_str, 'iloc') else tags_str[0]
        else:
            return {}
    
    try:
        # Remove extra quotes and parse JSON
        if isinstance(tags_str, str):
            # Handle double-quoted JSON strings
            if tags_str.startswith('"{') and tags_str.endswith('}"'):
                tags_str = tags_str[1:-1].replace('""', '"')
            return json.loads(tags_str)
    except (json.JSONDecodeError, ValueError, TypeError):
        return {}
    return tags_str if isinstance(tags_str, dict) else {}

def get_tag_values(df, tag_key):
    """Extract unique values for a specific tag key from the dataframe"""
    tag_values = set()
    if 'custom_tags' in df.columns:
        for tags_str in df['custom_tags'].dropna():
            try:
                tags = parse_tags(tags_str)
                if isinstance(tags, dict) and tag_key in tags:
                    tag_values.add(tags[tag_key])
            except Exception:
                # Skip rows with problematic tag data
                continue
    return sorted(list(tag_values))

def format_currency(value):
    """Format currency values consistently"""
    if pd.isna(value):
        return "$0.00"
    return f"${value:,.2f}"

def format_large_number(value):
    """Format large numbers with appropriate suffixes"""
    if pd.isna(value):
        return "0"
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K"
    else:
        return f"{value:.1f}"

def create_metric_card(title, value, delta=None, delta_color="normal"):
    """Create a consistent metric card display"""
    st.metric(
        label=title,
        value=value,
        delta=delta,
        delta_color=delta_color
    )

def apply_filters(df, filters):
    """Apply multiple filters to a dataframe"""
    filtered_df = df.copy()
    
    for column, values in filters.items():
        if values and column in filtered_df.columns:
            if isinstance(values, list):
                filtered_df = filtered_df[filtered_df[column].isin(values)]
            else:
                filtered_df = filtered_df[filtered_df[column] == values]
    
    return filtered_df

def create_bar_chart(data, x, y, title, color=DATABRICKS_COLORS[0]):
    """Create a standardized bar chart"""
    chart = alt.Chart(data).mark_bar(
        color=color
    ).encode(
        x=alt.X(f'{x}:N', title=x.replace('_', ' ').title()),
        y=alt.Y(f'{y}:Q', title=y.replace('_', ' ').title(), axis=alt.Axis(format='$.0f')),
        tooltip=[
            alt.Tooltip(f'{x}:N', title=x.replace('_', ' ').title()),
            alt.Tooltip(f'{y}:Q', title=y.replace('_', ' ').title(), format='$.2f')
        ]
    ).properties(
        title=title,
        width=400,
        height=300
    )
    
    return chart

def create_horizontal_bar_chart(data, x, y, title, color=DATABRICKS_COLORS[0]):
    """Create a standardized horizontal bar chart"""
    chart = alt.Chart(data).mark_bar(
        color=color
    ).encode(
        x=alt.X(f'{y}:Q', title=y.replace('_', ' ').title(), axis=alt.Axis(format='$.0f')),
        y=alt.Y(f'{x}:N', title=x.replace('_', ' ').title(), sort='-x'),
        tooltip=[
            alt.Tooltip(f'{x}:N', title=x.replace('_', ' ').title()),
            alt.Tooltip(f'{y}:Q', title=y.replace('_', ' ').title(), format='$.2f')
        ]
    ).properties(
        title=title,
        width=600,
        height=400
    )
    
    return chart

def create_line_chart(data, x, y, title, color=DATABRICKS_COLORS[0]):
    """Create a standardized line chart"""
    chart = alt.Chart(data).mark_line(
        color=color,
        strokeWidth=3,
        point=alt.OverlayMarkDef(
            color=color,
            size=60
        )
    ).encode(
        x=alt.X(f'{x}:T' if 'date' in x.lower() else f'{x}:N', title=x.replace('_', ' ').title()),
        y=alt.Y(f'{y}:Q', title=y.replace('_', ' ').title(), axis=alt.Axis(format='$.0f')),
        tooltip=[
            alt.Tooltip(f'{x}:N', title=x.replace('_', ' ').title()),
            alt.Tooltip(f'{y}:Q', title=y.replace('_', ' ').title(), format='$.2f')
        ]
    ).properties(
        title=title,
        width=400,
        height=300
    )
    
    return chart

def create_data_table(df, title, max_rows=100):
    """Create a standardized data table"""
    st.subheader(title)
    
    # Show summary
    st.write(f"Showing {min(len(df), max_rows)} of {len(df)} rows")
    
    # Display the table
    display_df = df.head(max_rows)
    
    # Format currency columns
    currency_cols = [col for col in display_df.columns if 'cost' in col.lower() or 'price' in col.lower()]
    for col in currency_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(format_currency)
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    return display_df