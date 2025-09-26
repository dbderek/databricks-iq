"""
Model Serving Analytics Page for Databricks IQ
"""

import streamlit as st
import pandas as pd
import altair as alt
import json
from datetime import timedelta
from utils import (
    load_data, parse_tags, get_tag_values, format_currency, 
    create_metric_card, apply_filters, create_bar_chart, create_horizontal_bar_chart,
    create_line_chart, create_data_table, DATABRICKS_COLORS
)

def show_model_serving_analytics():
    st.header("🤖 Model Serving Analytics")
    st.markdown("Analyze model serving and batch inference costs")
    
    # Create tabs for different model serving analytics sections
    tab1, tab2 = st.tabs([
        "Model Serving Costs", 
        "Batch Inference Costs"
    ])
    
    with tab1:
        show_model_serving_costs()
    
    with tab2:
        show_batch_inference_costs()

def show_model_serving_costs():
    """Show model serving costs analysis"""
    st.subheader("🚀 Model Serving Costs")
    
    # Load data
    serving_data = load_data('model_serving_costs.csv')
    if serving_data.empty:
        return
    
    # Create filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # User filter (created_by)
        users = serving_data['created_by'].dropna().unique()
        selected_users = st.multiselect("Filter by User (created_by)", users, key="serving_user_filter")
    
    with col2:
        # Tag filter
        all_tags = set()
        for tags_str in serving_data['custom_tags'].dropna():
            tags = parse_tags(tags_str)
            all_tags.update(tags.keys())
        
        selected_tag_key = st.selectbox("Filter by Tag Key", [''] + sorted(list(all_tags)), key="serving_tag_key")
        
        selected_tag_values = []
        if selected_tag_key:
            tag_values = get_tag_values(serving_data, selected_tag_key)
            selected_tag_values = st.multiselect(f"Filter by {selected_tag_key} Values", tag_values, key="serving_tag_values")
    
    with col3:
        # Endpoint name filter
        endpoints = serving_data['endpoint_name'].dropna().unique()
        selected_endpoints = st.multiselect("Filter by Endpoint Name", endpoints, key="serving_endpoint_filter")
        
        # Date filter
        if 'last_usage_date' in serving_data.columns:
            serving_data['last_usage_date'] = pd.to_datetime(serving_data['last_usage_date'])
            date_range = st.date_input(
                "Filter by Date Range",
                value=[],
                key="serving_date_filter"
            )
    
    # Apply filters
    filtered_data = serving_data.copy()
    
    if selected_users:
        filtered_data = filtered_data[filtered_data['created_by'].isin(selected_users)]
    
    if selected_endpoints:
        filtered_data = filtered_data[filtered_data['endpoint_name'].isin(selected_endpoints)]
    
    if selected_tag_key and selected_tag_values:
        mask = filtered_data['custom_tags'].apply(
            lambda x: selected_tag_key in parse_tags(x) and 
                     parse_tags(x).get(selected_tag_key) in selected_tag_values
        )
        filtered_data = filtered_data[mask]
    
    # Display metrics cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_endpoints = filtered_data['endpoint_name'].nunique()
        create_metric_card("Total Endpoints", f"{total_endpoints:,}")
    
    with col2:
        total_cost = filtered_data['total_effective_cost'].sum()
        create_metric_card("Total Cost", format_currency(total_cost))
    
    with col3:
        avg_cost = filtered_data['total_effective_cost'].mean()
        create_metric_card("Avg Cost per Endpoint", format_currency(avg_cost))
    
    with col4:
        t7d_cost = filtered_data['t7d_effective_cost'].sum()
        create_metric_card("7-Day Cost", format_currency(t7d_cost))
    
    # Charts section
    if not filtered_data.empty:
        # Entity type distribution pie chart
        type_costs = filtered_data.groupby('entity_type')['total_effective_cost'].sum().reset_index()
        
        # Create pie chart
        base = alt.Chart(type_costs)
        
        chart = base.mark_arc(
            innerRadius=50,
            stroke='white',
            strokeWidth=2
        ).encode(
            theta=alt.Theta('total_effective_cost:Q'),
            color=alt.Color('entity_type:N', 
                          scale=alt.Scale(range=DATABRICKS_COLORS),
                          legend=alt.Legend(title="Entity Type")),
            tooltip=[
                alt.Tooltip('entity_type:N', title='Type'),
                alt.Tooltip('total_effective_cost:Q', title='Cost', format='$.2f')
            ]
        ).properties(
            title="Cost Distribution by Entity Type",
            width=300,
            height=300
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.altair_chart(chart, use_container_width=True)
        
        with col2:
            # Cost distribution by endpoint_name pie chart
            endpoint_costs = filtered_data.groupby('endpoint_name')['total_effective_cost'].sum().reset_index()
            endpoint_costs = endpoint_costs.nlargest(10, 'total_effective_cost')  # Top 10 for readability
            
            chart = alt.Chart(endpoint_costs).mark_arc(
                innerRadius=50,
                stroke='white',
                strokeWidth=2
            ).encode(
                theta=alt.Theta('total_effective_cost:Q'),
                color=alt.Color('endpoint_name:N', 
                              scale=alt.Scale(range=DATABRICKS_COLORS),
                              legend=alt.Legend(title="Endpoint")),
                tooltip=[
                    alt.Tooltip('endpoint_name:N', title='Endpoint'),
                    alt.Tooltip('total_effective_cost:Q', title='Cost', format='$.2f')
                ]
            ).properties(
                title="Cost Distribution by Endpoint Name",
                width=300,
                height=300
            )
            
            st.altair_chart(chart, use_container_width=True)
        
        # Top 25 endpoints by cost - horizontal bar chart (moved below pie charts)
        top_endpoints = filtered_data.nlargest(25, 'total_effective_cost')
        
        chart = create_horizontal_bar_chart(
            top_endpoints, 'endpoint_name', 'total_effective_cost',
            'Top 25 Endpoints by Cost', DATABRICKS_COLORS[0]
        )
        st.altair_chart(chart, use_container_width=True)
    
    # Data table
    create_data_table(
        filtered_data.sort_values('total_effective_cost', ascending=False),
        "🤖 Top Model Serving Endpoints by Cost"
    )

def show_batch_inference_costs():
    """Show batch inference costs analysis"""
    st.subheader("⚡ Batch Inference Costs")
    
    # Load data
    batch_data = load_data('batch_inference_costs.csv')
    if batch_data.empty:
        st.warning("No batch inference costs data available")
        return
    
    # Determine cost column
    cost_column = None
    if 'effective_cost' in batch_data.columns:
        cost_column = 'effective_cost'
    elif 'cost' in batch_data.columns:
        cost_column = 'cost'
    
    if not cost_column:
        st.error("No cost column found in batch inference data")
        return
    
    # Show basic metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_entries = len(batch_data)
        create_metric_card("Total Entries", f"{total_entries:,}")
    
    with col2:
        total_cost = batch_data[cost_column].sum()
        create_metric_card("Total Cost", format_currency(total_cost))
    
    with col3:
        avg_cost = batch_data[cost_column].mean()
        create_metric_card("Average Cost", format_currency(avg_cost))
    
    # Charts section
    # Top 25 Users (run_as) by total costs - horizontal bar chart
    if 'run_as' in batch_data.columns:
        user_costs = batch_data.groupby('run_as')[cost_column].sum().reset_index()
        user_costs = user_costs.nlargest(25, cost_column)
        
        chart = create_horizontal_bar_chart(
            user_costs, 'run_as', cost_column,
            'Top 25 Users (run_as) by Total Costs', DATABRICKS_COLORS[0]
        )
        st.altair_chart(chart, use_container_width=True)
    
    # Cost by day for the past 30 days - stacked bar chart by endpoint_name
    if 'date' in batch_data.columns or any('date' in col.lower() for col in batch_data.columns):
        date_cols = [col for col in batch_data.columns if 'date' in col.lower()]
        if date_cols:
            date_col = date_cols[0]
            
            try:
                batch_data[date_col] = pd.to_datetime(batch_data[date_col])
                
                # Filter to past 30 days
                max_date = batch_data[date_col].max()
                from datetime import timedelta
                start_date = max_date - timedelta(days=30)
                recent_data = batch_data[batch_data[date_col] >= start_date]
                
                if len(recent_data) > 0 and 'endpoint_name' in recent_data.columns:
                    # Group by date and endpoint
                    daily_endpoint_costs = recent_data.groupby([date_col, 'endpoint_name'])[cost_column].sum().reset_index()
                    
                    # Create stacked bar chart
                    chart = alt.Chart(daily_endpoint_costs).mark_bar().encode(
                        x=alt.X(f'{date_col}:T', title='Date'),
                        y=alt.Y(f'{cost_column}:Q', title='Cost ($)', axis=alt.Axis(format='$.0f')),
                        color=alt.Color('endpoint_name:N', 
                                       scale=alt.Scale(range=DATABRICKS_COLORS),
                                       legend=alt.Legend(title="Endpoint")),
                        tooltip=[
                            alt.Tooltip(f'{date_col}:T', title='Date'),
                            alt.Tooltip('endpoint_name:N', title='Endpoint'),
                            alt.Tooltip(f'{cost_column}:Q', title='Cost', format='$.2f')
                        ]
                    ).properties(
                        title='Cost by Day (Past 30 Days) - Split by Endpoint',
                        width=800,
                        height=400
                    )
                    
                    st.altair_chart(chart, use_container_width=True)
                    
            except Exception as e:
                st.warning(f"Could not parse date column {date_col}: {str(e)}")
    
    # Full data table
    create_data_table(
        batch_data.sort_values(cost_column, ascending=False),
        "⚡ Batch Inference Costs"
    )

if __name__ == "__main__":
    show_model_serving_analytics()