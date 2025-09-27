"""
Utilities for Databricks IQ application
"""

import streamlit as st
import pandas as pd
import altair as alt
import json
import warnings
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Suppress warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Configure Altair
alt.data_transformers.disable_max_rows()

# Databricks color palette
DATABRICKS_COLORS = ['#FF3621', '#00A1F1', '#7C4DFF', '#00D4AA', '#FF8A00', '#E91E63', '#9C27B0', '#673AB7']

def load_data(filename: str, use_live_data: bool = False, http_path: Optional[str] = None, 
              filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Load data from either CSV files or live Databricks SQL
    
    Args:
        filename: Name of the CSV file or table
        use_live_data: Whether to use live Databricks SQL or CSV files
        http_path: HTTP path to Databricks SQL warehouse (optional, uses env var if not provided)
        filters: Optional filters to apply to the query
    
    Returns:
        pandas DataFrame with the data
    """
    if use_live_data:
        return load_live_data(filename, http_path, filters)
    else:
        return load_csv_data(filename)

def load_csv_data(filename: str) -> pd.DataFrame:
    """Load CSV data from the example_data directory"""
    try:
        data_path = Path(__file__).parent / 'example_data' / filename
        df = pd.read_csv(data_path)
        return df
    except FileNotFoundError:
        st.error(f"Could not find data file: {filename}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading {filename}: {str(e)}")
        return pd.DataFrame()

def load_live_data(filename: str, http_path: Optional[str] = None, filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Load data from live Databricks SQL
    
    Args:
        filename: CSV filename to convert to table name
        http_path: HTTP path to Databricks SQL warehouse (optional, uses env var if not provided)
        filters: Optional filters to apply
    
    Returns:
        pandas DataFrame with the data, falls back to CSV if SQL fails
    """
    try:
        from databricks_client import get_databricks_client, get_table_name_from_filename
        from config import DATA_CONFIG
        
        # Get Databricks client
        client = get_databricks_client(
            catalog=DATA_CONFIG["databricks"]["catalog"],
            schema=DATA_CONFIG["databricks"]["schema"]
        )
        
        # Convert filename to table name
        table_name = get_table_name_from_filename(filename)
        
        # Query the data
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
            st.warning(f"⚠️ No data returned from SQL table {table_name}, falling back to CSV")
            return load_csv_data(filename)
            
    except ImportError:
        st.warning("⚠️ Databricks client not available, using CSV data")
        return load_csv_data(filename)
    except Exception as e:
        st.error(f"❌ Error loading from Databricks SQL: {str(e)}")
        st.info("📁 Falling back to CSV data")
        return load_csv_data(filename)

def get_data_source_info(use_live_data: bool, http_path: Optional[str] = None) -> Dict[str, str]:
    """
    Get information about the current data source
    
    Args:
        use_live_data: Whether using live data
        http_path: HTTP path if using live data (optional, uses env var if not provided)
    
    Returns:
        Dictionary with data source information
    """
    if use_live_data:
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
        "source": "CSV Files",
        "location": "example_data/",
        "type": "Static sample data"
    }

def parse_tags(tags_str):
    """Parse custom tags JSON string into a readable format"""
    if pd.isna(tags_str) or tags_str == '':
        return {}
    try:
        # Remove extra quotes and parse JSON
        if isinstance(tags_str, str):
            # Handle double-quoted JSON strings
            if tags_str.startswith('"{') and tags_str.endswith('}"'):
                tags_str = tags_str[1:-1].replace('""', '"')
            return json.loads(tags_str)
    except (json.JSONDecodeError, ValueError):
        return {}
    return tags_str if isinstance(tags_str, dict) else {}

def get_tag_values(df, tag_key):
    """Extract unique values for a specific tag key from the dataframe"""
    tag_values = set()
    if 'custom_tags' in df.columns:
        for tags_str in df['custom_tags'].dropna():
            tags = parse_tags(tags_str)
            if tag_key in tags:
                tag_values.add(tags[tag_key])
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
    
    st.dataframe(display_df, width="stretch", hide_index=True)
    
    return display_df