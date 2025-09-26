"""
Serverless Analytics Page for Databricks IQ
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

def show_serverless_analytics():
    st.header("⚡ Serverless Analytics")
    st.markdown("Analyze serverless compute costs for jobs and notebooks")
    
    # Create tabs for different serverless analytics sections
    tab1, tab2, tab3 = st.tabs([
        "Serverless Job Spend", 
        "Serverless Notebook Spend", 
        "Consumption by Tag"
    ])
    
    with tab1:
        show_serverless_job_spend()
    
    with tab2:
        show_serverless_notebook_spend()
    
    with tab3:
        show_serverless_consumption_by_tag()

def show_serverless_job_spend():
    """Show serverless job spending analysis"""
    st.subheader("💼 Serverless Job Spend")
    
    # Load data
    job_data = load_data('serverless_job_spend.csv')
    if job_data.empty:
        return
    
    # Display metrics cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_jobs = len(job_data)
        create_metric_card("Total Jobs", f"{total_jobs:,}")
    
    with col2:
        total_cost = job_data['effective_cost'].sum()
        create_metric_card("Total Cost", format_currency(total_cost))
    
    with col3:
        total_dbu = job_data['total_dbu'].sum()
        create_metric_card("Total DBUs", f"{total_dbu:,.1f}")
    
    with col4:
        avg_cost_per_job = job_data['effective_cost'].mean()
        create_metric_card("Avg Cost per Job", format_currency(avg_cost_per_job))
    
    # Charts section
    if not job_data.empty:
        # Top 25 jobs by cost - horizontal bar chart
        top_jobs = job_data.nlargest(25, 'effective_cost')
        
        # Use job_id for display if job_name is null
        top_jobs['display_name'] = top_jobs.apply(
            lambda row: row['job_name'] if pd.notna(row['job_name']) and row['job_name'] != '' 
            else f"Job ID: {row['job_id']}", axis=1
        )
        
        chart = create_horizontal_bar_chart(
            top_jobs, 'display_name', 'effective_cost',
            'Top 25 Serverless Jobs by Cost', DATABRICKS_COLORS[0]
        )
        st.altair_chart(chart, use_container_width=True)
    
    # Data table
    create_data_table(
        job_data.sort_values('effective_cost', ascending=False),
        "💰 Serverless Jobs by Cost"
    )

def show_serverless_notebook_spend():
    """Show serverless notebook spending analysis"""
    st.subheader("📓 Serverless Notebook Spend")
    
    # Load data
    notebook_data = load_data('serverless_notebook_spend.csv')
    if notebook_data.empty:
        return
    
    # Display metrics cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_notebooks = len(notebook_data)
        create_metric_card("Total Notebooks", f"{total_notebooks:,}")
    
    with col2:
        total_cost = notebook_data['effective_cost'].sum() if 'effective_cost' in notebook_data.columns else 0
        create_metric_card("Total Cost", format_currency(total_cost))
    
    with col3:
        total_dbu = notebook_data['total_dbu'].sum() if 'total_dbu' in notebook_data.columns else 0
        create_metric_card("Total DBUs", f"{total_dbu:,.1f}")
    
    with col4:
        avg_cost = notebook_data['effective_cost'].mean() if 'effective_cost' in notebook_data.columns else 0
        create_metric_card("Avg Cost per Notebook", format_currency(avg_cost))
    
    # Charts section
    if not notebook_data.empty and 'effective_cost' in notebook_data.columns:
        # Top 25 notebook users by cost - horizontal bar chart
        if 'run_as' in notebook_data.columns:
            user_costs = notebook_data.groupby('run_as')['effective_cost'].sum().reset_index()
            user_costs = user_costs.nlargest(25, 'effective_cost')
            
            chart = create_horizontal_bar_chart(
                user_costs, 'run_as', 'effective_cost',
                'Top 25 Notebook Users (run_as) by Cost', DATABRICKS_COLORS[1]
            )
            st.altair_chart(chart, use_container_width=True)
    
    # Full data table
    create_data_table(
        notebook_data.sort_values('effective_cost', ascending=False) if 'effective_cost' in notebook_data.columns else notebook_data,
        "📓 All Serverless Notebooks"
    )

def show_serverless_consumption_by_tag():
    """Show serverless consumption analysis by tags"""
    st.subheader("🏷️ Consumption by Tag")
    
    # Load data
    tag_data = load_data('serverless_consumption_by_tag.csv')
    if tag_data.empty:
        return
    
    # Tag filter
    all_tags = set()
    if 'custom_tags' in tag_data.columns:
        for tags_str in tag_data['custom_tags'].dropna():
            tags = parse_tags(tags_str)
            all_tags.update(tags.keys())
    
    selected_tag_key = st.selectbox("Filter by Tag Key", [''] + sorted(list(all_tags)), key="serverless_tag_key")
    
    selected_tag_values = []
    if selected_tag_key:
        tag_values = get_tag_values(tag_data, selected_tag_key)
        selected_tag_values = st.multiselect(f"Filter by {selected_tag_key} Values", tag_values, key="serverless_tag_values")
    
    # Apply tag filter
    filtered_data = tag_data.copy()
    if selected_tag_key and selected_tag_values:
        mask = filtered_data['custom_tags'].apply(
            lambda x: selected_tag_key in parse_tags(x) and 
                     parse_tags(x).get(selected_tag_key) in selected_tag_values
        )
        filtered_data = filtered_data[mask]
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_entries = len(filtered_data)
        create_metric_card("Total Entries", f"{total_entries:,}")
    
    with col2:
        if 'effective_cost' in filtered_data.columns:
            total_cost = filtered_data['effective_cost'].sum()
            create_metric_card("Total Cost", format_currency(total_cost))
    
    with col3:
        if 'total_dbu' in filtered_data.columns:
            total_dbu = filtered_data['total_dbu'].sum()
            create_metric_card("Total DBUs", f"{total_dbu:,.1f}")
    
    # Unpack custom tags and create individual tag analysis
    if 'custom_tags' in filtered_data.columns and 'effective_cost' in filtered_data.columns:
        # Create a list to store individual tag records
        tag_records = []
        
        for _, row in filtered_data.iterrows():
            tags = parse_tags(row['custom_tags'])
            for tag_key, tag_value in tags.items():
                tag_records.append({
                    'tag_key': tag_key,
                    'tag_value': tag_value,
                    'effective_cost': row['effective_cost'],
                    'total_dbu': row.get('total_dbu', 0)
                })
        
        if tag_records:
            tag_df = pd.DataFrame(tag_records)
            
            # Chart: Top 25 tag values by cost
            if selected_tag_key and selected_tag_key in tag_df['tag_key'].values:
                tag_costs = tag_df[tag_df['tag_key'] == selected_tag_key].groupby('tag_value')['effective_cost'].sum().reset_index()
                tag_costs = tag_costs.nlargest(25, 'effective_cost')
                
                chart = create_horizontal_bar_chart(
                    tag_costs, 'tag_value', 'effective_cost',
                    f'Top 25 {selected_tag_key} Values by Cost', DATABRICKS_COLORS[2]
                )
                st.altair_chart(chart, use_container_width=True)
            
            else:
                # Show top tag keys by cost
                tag_key_costs = tag_df.groupby('tag_key')['effective_cost'].sum().reset_index()
                tag_key_costs = tag_key_costs.nlargest(25, 'effective_cost')
                
                chart = create_horizontal_bar_chart(
                    tag_key_costs, 'tag_key', 'effective_cost',
                    'Top 25 Tag Keys by Cost', DATABRICKS_COLORS[2]
                )
                st.altair_chart(chart, use_container_width=True)
    
    # Full data table
    create_data_table(
        filtered_data.sort_values('effective_cost', ascending=False) if 'effective_cost' in filtered_data.columns else filtered_data,
        "🏷️ Serverless Consumption by Tag"
    )

if __name__ == "__main__":
    show_serverless_analytics()