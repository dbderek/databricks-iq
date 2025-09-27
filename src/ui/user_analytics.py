"""
User Analytics Page for Databricks IQ
"""

import streamlit as st
import pandas as pd
import altair as alt
import json
from utils import (
    load_data, parse_tags, get_tag_values, format_currency, 
    create_metric_card, apply_filters, create_bar_chart, create_horizontal_bar_chart,
    create_line_chart, create_data_table, DATABRICKS_COLORS
)

def show_user_analytics():
    st.header("👥 User Analytics")
    st.markdown("Analyze user consumption patterns and spending alerts")
    
    # Create tabs for different user analytics sections (reordered)
    tab1, tab2 = st.tabs([
        "User Serverless Spend", 
        "User Serverless Spend Details"
    ])
    
    with tab1:
        show_user_spend_alerts()  # First tab is now User Spend Alerts (renamed)
    
    with tab2:
        show_user_serverless_consumption()  # Second tab is now User Serverless Consumption (renamed)

def show_user_serverless_consumption():
    """Show user serverless consumption analysis - renamed to User Serverless Spend Details"""
    st.subheader("⚡ User Serverless Spend Details")
    
    # Get data source configuration from session state
    config = st.session_state.get('data_source_config', {'use_live_data': False, 'http_path': ''})
    
    # Load data
    consumption_data = load_data('user_serverless_consumption.csv',
                                use_live_data=config['use_live_data'], 
                                http_path=config['http_path'])
    if consumption_data.empty:
        st.warning("No user serverless consumption data available")
        return
    
    # Create filters
    col1, col2 = st.columns(2)
    
    with col1:
        # User filter (run_as)
        users = consumption_data['run_as'].dropna().unique()
        selected_users = st.multiselect("Filter by User (run_as)", users, key="user_consumption_filter")
    
    with col2:
        # Job/Notebook filter
        if 'job_name' in consumption_data.columns:
            job_names = consumption_data['job_name'].dropna().unique()
            selected_jobs = st.multiselect("Filter by Job Name", job_names, key="user_job_filter")
    
    # Apply filters
    filtered_data = consumption_data.copy()
    
    if selected_users:
        filtered_data = filtered_data[filtered_data['run_as'].isin(selected_users)]
    
    if 'job_name' in consumption_data.columns and 'selected_jobs' in locals() and selected_jobs:
        filtered_data = filtered_data[filtered_data['job_name'].isin(selected_jobs)]
    
    # Display metrics cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        unique_users = filtered_data['run_as'].nunique()
        create_metric_card("Unique Users", f"{unique_users:,}")
    
    with col2:
        total_cost = filtered_data['effective_cost'].sum()
        create_metric_card("Total Cost", format_currency(total_cost))
    
    with col3:
        if 'total_dbu' in filtered_data.columns:
            total_dbu = filtered_data['total_dbu'].sum()
            create_metric_card("Total DBUs", f"{total_dbu:,.1f}")
    
    with col4:
        avg_cost_per_user = filtered_data.groupby('run_as')['effective_cost'].sum().mean()
        create_metric_card("Avg Cost per User", format_currency(avg_cost_per_user))
    
    # Charts section
    if not filtered_data.empty:
        col1, col2 = st.columns([3, 1])  # 75% and 25% column widths
        
        with col1:
            # Top 25 users by cost - horizontal bar chart (75% width)
            user_costs = filtered_data.groupby('run_as')['effective_cost'].sum().reset_index()
            user_costs = user_costs.nlargest(25, 'effective_cost')
            
            chart = create_horizontal_bar_chart(
                user_costs, 'run_as', 'effective_cost',
                'Top 25 Users by Cost', DATABRICKS_COLORS[0]
            )
            st.altair_chart(chart, use_container_width=True)
        
        with col2:
            # Pie chart for cost distribution (25% width)
            # User consumption breakdown - show resource type distribution if possible
            if 'resource_type' in filtered_data.columns:
                resource_costs = filtered_data.groupby('resource_type')['effective_cost'].sum().reset_index()
                
                chart = alt.Chart(resource_costs).mark_arc(
                    innerRadius=30,
                    stroke='white',
                    strokeWidth=2
                ).encode(
                    theta=alt.Theta('effective_cost:Q'),
                    color=alt.Color('resource_type:N', 
                                  scale=alt.Scale(range=DATABRICKS_COLORS),
                                  legend=alt.Legend(title="Resource Type")),
                    tooltip=[
                        alt.Tooltip('resource_type:N', title='Type'),
                        alt.Tooltip('effective_cost:Q', title='Cost', format='$.2f')
                    ]
                ).properties(
                    title="Cost by Resource Type",
                    width=200,
                    height=200
                )
                
                st.altair_chart(chart, use_container_width=True)
            
            elif 'job_id' in filtered_data.columns and 'notebook_id' in filtered_data.columns:
                # Job vs Notebook consumption
                job_consumption = filtered_data[filtered_data['job_id'].notna()]
                notebook_consumption = filtered_data[filtered_data['notebook_id'].notna()]
                
                breakdown_data = pd.DataFrame({
                    'Type': ['Jobs', 'Notebooks'],
                    'Cost': [
                        job_consumption['effective_cost'].sum(),
                        notebook_consumption['effective_cost'].sum()
                    ]
                })
                
                chart = alt.Chart(breakdown_data).mark_arc(
                    innerRadius=30,
                    stroke='white',
                    strokeWidth=2
                ).encode(
                    theta=alt.Theta('Cost:Q'),
                    color=alt.Color('Type:N', 
                                  scale=alt.Scale(range=DATABRICKS_COLORS[:2]),
                                  legend=alt.Legend(title="Resource Type")),
                    tooltip=[
                        alt.Tooltip('Type:N', title='Type'),
                        alt.Tooltip('Cost:Q', title='Cost', format='$.2f')
                    ]
                ).properties(
                    title="Cost by Resource Type",
                    width=200,
                    height=200
                )
                
                st.altair_chart(chart, use_container_width=True)
    
    # Data table
    create_data_table(
        filtered_data.sort_values('effective_cost', ascending=False),
        "👥 User Serverless Consumption Details"
    )

def show_user_spend_alerts():
    """Show user spend alerts analysis - renamed to User Serverless Spend"""
    st.subheader("🚨 User Serverless Spend")
    
    # Get data source configuration from session state
    config = st.session_state.get('data_source_config', {'use_live_data': False, 'http_path': ''})
    
    # Load data
    alerts_data = load_data('user_spend_alerts.csv',
                           use_live_data=config['use_live_data'], 
                           http_path=config['http_path'])
    if alerts_data.empty:
        st.warning("No user spend alerts data available")
        return
    
    # Create filters
    if 'user' in alerts_data.columns or 'run_as' in alerts_data.columns:
        user_col = 'user' if 'user' in alerts_data.columns else 'run_as'
        users = alerts_data[user_col].dropna().unique()
        selected_users = st.multiselect(f"Filter by User ({user_col})", users, key="alerts_user_filter")
        
        # Apply filters
        filtered_data = alerts_data.copy()
        if selected_users:
            filtered_data = filtered_data[filtered_data[user_col].isin(selected_users)]
    else:
        filtered_data = alerts_data.copy()
    
    # Display metrics cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_alerts = len(filtered_data)
        create_metric_card("Total Entries", f"{total_alerts:,}")
    
    with col2:
        if 'user' in filtered_data.columns or 'run_as' in filtered_data.columns:
            user_col = 'user' if 'user' in filtered_data.columns else 'run_as'
            unique_users = filtered_data[user_col].nunique()
            create_metric_card("Unique Users", f"{unique_users:,}")
    
    with col3:
        # Look for cost columns
        cost_columns = ['total_effective_cost', 'total_cost', 't7d_effective_cost', 't14d_effective_cost', 'amount', 'cost', 'effective_cost']
        cost_col = None
        for col in cost_columns:
            if col in filtered_data.columns:
                cost_col = col
                break
        
        if cost_col:
            total_cost = filtered_data[cost_col].sum()
            create_metric_card(f"Total {cost_col.replace('_', ' ').title()}", format_currency(total_cost))
    
    # Charts: Top 25 spending users for total, t7d, and t14d (horizontal bar charts, full screen width)
    if cost_col and 'user' in filtered_data.columns or 'run_as' in filtered_data.columns:
        user_col = 'user' if 'user' in filtered_data.columns else 'run_as'
        
        # Total spending
        if 'total_effective_cost' in filtered_data.columns or cost_col == 'total_effective_cost':
            user_costs = filtered_data.groupby(user_col)['total_effective_cost' if 'total_effective_cost' in filtered_data.columns else cost_col].sum().reset_index()
            user_costs = user_costs.nlargest(25, 'total_effective_cost' if 'total_effective_cost' in filtered_data.columns else cost_col)
            
            chart = create_horizontal_bar_chart(
                user_costs, user_col, 'total_effective_cost' if 'total_effective_cost' in filtered_data.columns else cost_col,
                'Top 25 Spending Users - Total', DATABRICKS_COLORS[0]
            )
            st.altair_chart(chart, use_container_width=True)
        
        # T7D spending
        if 't7d_effective_cost' in filtered_data.columns:
            user_costs_t7d = filtered_data.groupby(user_col)['t7d_effective_cost'].sum().reset_index()
            user_costs_t7d = user_costs_t7d.nlargest(25, 't7d_effective_cost')
            
            chart = create_horizontal_bar_chart(
                user_costs_t7d, user_col, 't7d_effective_cost',
                'Top 25 Spending Users - T7D', DATABRICKS_COLORS[1]
            )
            st.altair_chart(chart, use_container_width=True)
        
        # T14D spending
        if 't14d_effective_cost' in filtered_data.columns:
            user_costs_t14d = filtered_data.groupby(user_col)['t14d_effective_cost'].sum().reset_index()
            user_costs_t14d = user_costs_t14d.nlargest(25, 't14d_effective_cost')
            
            chart = create_horizontal_bar_chart(
                user_costs_t14d, user_col, 't14d_effective_cost',
                'Top 25 Spending Users - T14D', DATABRICKS_COLORS[2]
            )
            st.altair_chart(chart, use_container_width=True)
    
    # Full data table
    create_data_table(
        filtered_data,
        "🚨 User Serverless Spend Data"
    )

if __name__ == "__main__":
    show_user_analytics()