"""
Job Analytics Page for Databricks IQ
"""

import streamlit as st
import pandas as pd
import altair as alt
import json
from datetime import timedelta
from utils import (
    load_data, parse_tags, get_tag_values, format_currency, is_empty_string,
    create_metric_card, apply_filters, create_bar_chart, create_horizontal_bar_chart,
    create_line_chart, create_data_table, DATABRICKS_COLORS
)

def show_job_analytics():
    st.header("💼 Job Analytics")
    st.markdown("Analyze job costs and performance across your Databricks workspaces")
    
    # Create tabs for different job analytics sections
    tab1, tab2, tab3, tab4 = st.tabs([
        "Most Expensive Jobs", 
        "Most Expensive Job Runs", 
        "Job Spend Trends", 
        "Failed Jobs Analysis"
    ])
    
    with tab1:
        show_most_expensive_jobs()
    
    with tab2:
        show_most_expensive_job_runs()
    
    with tab3:
        show_job_spend_trends()
    
    with tab4:
        show_failed_jobs_analysis()

def show_most_expensive_jobs():
    """Show analysis of most expensive jobs"""
    st.subheader("🏆 Most Expensive Jobs")
    
    # Get data source configuration from session state
    config = st.session_state.get('data_source_config', {'http_path': None})
    
    # Load data
    jobs_data = load_data('most_expensive_jobs', 
                         http_path=config['http_path'])
    if jobs_data.empty:
        st.warning("No data available for most expensive jobs")
        return
    
    # Create filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # User filter
        users = jobs_data['run_as'].dropna().unique()
        selected_users = st.multiselect("Filter by User (run_as)", users, key="jobs_user_filter")
    
    with col2:
        # Job ID filter
        job_ids = jobs_data['job_id'].dropna().unique()
        selected_job_ids = st.multiselect("Filter by Job ID", job_ids, key="jobs_id_filter")
    
    with col3:
        # Tag filter
        all_tags = set()
        for tags_str in jobs_data['custom_tags'].dropna():
            tags = parse_tags(tags_str)
            all_tags.update(tags.keys())
        
        selected_tag_key = st.selectbox("Filter by Tag Key", [''] + sorted(list(all_tags)), key="jobs_tag_key")
        
        selected_tag_values = []
        if selected_tag_key:
            tag_values = get_tag_values(jobs_data, selected_tag_key)
            selected_tag_values = st.multiselect(f"Filter by {selected_tag_key} Values", tag_values, key="jobs_tag_values")
    
    with col4:
        # Job name filter
        job_names = jobs_data['name'].dropna().unique()
        selected_job_names = st.multiselect("Filter by Job Name", job_names, key="jobs_name_filter")
        
        # Workspace filter
        workspaces = jobs_data['workspace_id'].dropna().unique()
        selected_workspaces = st.multiselect("Filter by Workspace ID", workspaces, key="jobs_workspace_filter")
    
    # Apply filters
    filtered_data = jobs_data.copy()
    
    if selected_users:
        filtered_data = filtered_data[filtered_data['run_as'].isin(selected_users)]
    
    if selected_job_ids:
        filtered_data = filtered_data[filtered_data['job_id'].isin(selected_job_ids)]
    
    if selected_job_names:
        filtered_data = filtered_data[filtered_data['name'].isin(selected_job_names)]
        
    if selected_workspaces:
        filtered_data = filtered_data[filtered_data['workspace_id'].isin(selected_workspaces)]
    
    if selected_tag_key and selected_tag_values:
        mask = filtered_data['custom_tags'].apply(
            lambda x: selected_tag_key in parse_tags(x) and 
                     parse_tags(x).get(selected_tag_key) in selected_tag_values
        )
        filtered_data = filtered_data[mask]
    
    # Display metrics cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_jobs = len(filtered_data)
        create_metric_card("Total Jobs", f"{total_jobs:,}")
    
    with col2:
        total_cost = filtered_data['effective_cost'].sum()
        create_metric_card("Total Cost", format_currency(total_cost))
    
    with col3:
        avg_cost = filtered_data['effective_cost'].mean()
        create_metric_card("Average Cost per Job", format_currency(avg_cost))
    
    with col4:
        total_runs = filtered_data['runs'].sum()
        create_metric_card("Total Runs", f"{total_runs:,}")
    
    # Charts section - stacked vertically
    if not filtered_data.empty:
        # Top 25 jobs by cost - horizontal bar chart
        top_jobs = filtered_data.nlargest(25, 'effective_cost')
        
        # Use job_id for display if name is null
        top_jobs['display_name'] = top_jobs.apply(
            lambda row: row['name'] if pd.notna(row['name']) and str(row['name']).strip() != '' 
            else f"Job ID: {row['job_id']}", axis=1
        )
        
        chart = create_horizontal_bar_chart(
            top_jobs, 'display_name', 'effective_cost',
            'Top 25 Jobs by Cost', DATABRICKS_COLORS[0]
        )
        st.altair_chart(chart, use_container_width=True)
        
        # Top 25 users by job cost - horizontal bar chart
        user_costs = filtered_data.groupby('run_as')['effective_cost'].sum().reset_index()
        user_costs = user_costs.nlargest(25, 'effective_cost')
        
        chart = create_horizontal_bar_chart(
            user_costs, 'run_as', 'effective_cost',
            'Top 25 Users by Job Cost', DATABRICKS_COLORS[1]
        )
        st.altair_chart(chart, use_container_width=True)
        
        # Jobs with most runs - horizontal bar chart
        job_run_counts = filtered_data.groupby(['job_id', 'name'])['runs'].first().reset_index()
        job_run_counts = job_run_counts.nlargest(25, 'runs')
        
        # Use job_id for display if name is null
        job_run_counts['display_name'] = job_run_counts.apply(
            lambda row: row['name'] if pd.notna(row['name']) and str(row['name']).strip() != '' 
            else f"Job ID: {row['job_id']}", axis=1
        )
        
        chart = create_horizontal_bar_chart(
            job_run_counts, 'display_name', 'runs',
            'Top 25 Jobs with Most Runs', DATABRICKS_COLORS[2]
        )
        st.altair_chart(chart, use_container_width=True)
    
    # Data table
    create_data_table(
        filtered_data.sort_values('effective_cost', ascending=False),
        "💰 Top 100 Most Expensive Jobs"
    )

def show_most_expensive_job_runs():
    """Show analysis of most expensive job runs"""
    st.subheader("🏃 Most Expensive Job Runs")
    
    # Get data source configuration from session state
    config = st.session_state.get('data_source_config', {'http_path': None})
    
    # Load data
    runs_data = load_data('most_expensive_job_runs',
                         http_path=config['http_path'])
    if runs_data.empty:
        st.warning("No data available for most expensive job runs")
        return
    
    # Create filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # User filter
        users = runs_data['run_as'].dropna().unique()
        selected_users = st.multiselect("Filter by User (run_as)", users, key="runs_user_filter")
    
    with col2:
        # Job ID filter
        job_ids = runs_data['job_id'].dropna().unique()
        selected_job_ids = st.multiselect("Filter by Job ID", job_ids, key="runs_job_id_filter")
    
    with col3:
        # Tag filter
        all_tags = set()
        for tags_str in runs_data['custom_tags'].dropna():
            tags = parse_tags(tags_str)
            all_tags.update(tags.keys())
        
        selected_tag_key = st.selectbox("Filter by Tag Key", [''] + sorted(list(all_tags)), key="runs_tag_key")
        
        selected_tag_values = []
        if selected_tag_key:
            tag_values = get_tag_values(runs_data, selected_tag_key)
            selected_tag_values = st.multiselect(f"Filter by {selected_tag_key} Values", tag_values, key="runs_tag_values")
    
    with col4:
        # Job name filter
        job_names = runs_data['name'].dropna().unique()
        selected_job_names = st.multiselect("Filter by Job Name", job_names, key="runs_name_filter")
        
        # Workspace filter
        workspaces = runs_data['workspace_id'].dropna().unique()
        selected_workspaces = st.multiselect("Filter by Workspace ID", workspaces, key="runs_workspace_filter")
    
    # Apply filters
    filtered_data = runs_data.copy()
    
    if selected_users:
        filtered_data = filtered_data[filtered_data['run_as'].isin(selected_users)]
    
    if selected_job_ids:
        filtered_data = filtered_data[filtered_data['job_id'].isin(selected_job_ids)]
    
    if selected_job_names:
        filtered_data = filtered_data[filtered_data['name'].isin(selected_job_names)]
        
    if selected_workspaces:
        filtered_data = filtered_data[filtered_data['workspace_id'].isin(selected_workspaces)]
    
    if selected_tag_key and selected_tag_values:
        mask = filtered_data['custom_tags'].apply(
            lambda x: selected_tag_key in parse_tags(x) and 
                     parse_tags(x).get(selected_tag_key) in selected_tag_values
        )
        filtered_data = filtered_data[mask]
    
    # Display metrics cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_runs = len(filtered_data)
        create_metric_card("Total Runs", f"{total_runs:,}")
    
    with col2:
        total_cost = filtered_data['effective_cost'].sum()
        create_metric_card("Total Cost", format_currency(total_cost))
    
    with col3:
        avg_cost = filtered_data['effective_cost'].mean()
        create_metric_card("Average Cost per Run", format_currency(avg_cost))
    
    with col4:
        unique_jobs = filtered_data['job_id'].nunique()
        create_metric_card("Unique Jobs", f"{unique_jobs:,}")
    
    # Charts section - stacked vertically
    if not filtered_data.empty:
        # Top 25 runs by cost - horizontal bar chart
        top_runs = filtered_data.nlargest(25, 'effective_cost')
        
        # Create display label for runs
        top_runs['display_label'] = top_runs.apply(
            lambda row: f"Run {row['run_id']}" if pd.isna(row['name']) or str(row['name']).strip() == '' 
            else f"{str(row['name'])[:20]}...", axis=1
        )
        
        chart = create_horizontal_bar_chart(
            top_runs, 'display_label', 'effective_cost',
            'Top 25 Runs by Cost', DATABRICKS_COLORS[2]
        )
        st.altair_chart(chart, use_container_width=True)
        
        # Top 25 users by run cost - horizontal bar chart
        user_costs = filtered_data.groupby('run_as')['effective_cost'].sum().reset_index()
        user_costs = user_costs.nlargest(25, 'effective_cost')
        
        chart = create_horizontal_bar_chart(
            user_costs, 'run_as', 'effective_cost',
            'Top 25 Users by Run Cost', DATABRICKS_COLORS[1]
        )
        st.altair_chart(chart, use_container_width=True)
        
        # Jobs with most runs - horizontal bar chart (count, not cost)
        job_run_counts = filtered_data.groupby('job_id').agg({
            'run_id': 'count',
            'effective_cost': 'sum'
        }).reset_index()
        job_run_counts.columns = ['job_id', 'run_count', 'total_cost']
        job_run_counts = job_run_counts.nlargest(25, 'run_count')
        
        # Create chart without $ formatting for run counts
        chart = alt.Chart(job_run_counts).mark_bar(
            color=DATABRICKS_COLORS[3]
        ).encode(
            x=alt.X('run_count:Q', title='Number of Runs'),
            y=alt.Y('job_id:N', title='Job ID', sort='-x'),
            tooltip=[
                alt.Tooltip('job_id:N', title='Job ID'),
                alt.Tooltip('run_count:Q', title='Run Count'),
                alt.Tooltip('total_cost:Q', title='Total Cost', format='$.2f')
            ]
        ).properties(
            title='Top 25 Jobs with Most Runs',
            width=600,
            height=400
        )
        
        st.altair_chart(chart, use_container_width=True)
    
    # Data table
    create_data_table(
        filtered_data.sort_values('effective_cost', ascending=False),
        "🏃‍♂️ Top 100 Most Expensive Job Runs"
    )

def show_job_spend_trends():
    """Show job spending trends over time"""
    st.subheader("📈 Job Spend Trends")
    
    # Get data source configuration from session state
    config = st.session_state.get('data_source_config', {'http_path': None})
    
    # Load data
    trend_data = load_data('job_spend_trend',
                          http_path=config['http_path'])
    if trend_data.empty:
        st.warning("No job spend trend data available")
        return
    
    # Check if date column exists
    date_cols = [col for col in trend_data.columns if 'date' in col.lower()]
    if not date_cols:
        st.error("No date column found in job spend trend data")
        return
        
    date_col = date_cols[0]
    
    # Parse date column
    try:
        trend_data[date_col] = pd.to_datetime(trend_data[date_col])
    except:
        st.error(f"Cannot parse date column: {date_col}")
        return
    
    # Calculate T7D, T14D, T30D costs
    current_date = trend_data[date_col].max()
    
    cost_col = 'effective_cost' if 'effective_cost' in trend_data.columns else 'cost'
    if cost_col not in trend_data.columns:
        cost_cols = [col for col in trend_data.columns if 'cost' in col.lower()]
        if cost_cols:
            cost_col = cost_cols[0]
        else:
            st.error("No cost column found")
            return
    
    # Filter data for time periods
    t7d_data = trend_data[trend_data[date_col] >= (current_date - timedelta(days=7))]
    t14d_data = trend_data[trend_data[date_col] >= (current_date - timedelta(days=14))]
    t30d_data = trend_data[trend_data[date_col] >= (current_date - timedelta(days=30))]
    
    # Display time period cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        t7d_cost = t7d_data[cost_col].sum()
        create_metric_card("T7D Costs", format_currency(t7d_cost))
    
    with col2:
        t14d_cost = t14d_data[cost_col].sum()
        create_metric_card("T14D Costs", format_currency(t14d_cost))
    
    with col3:
        t30d_cost = t30d_data[cost_col].sum()
        create_metric_card("T30D Costs", format_currency(t30d_cost))
    
    # Calculate job growth (if possible with available data)
    if 'job_id' in trend_data.columns:
        # Group by job and calculate growth
        job_costs = trend_data.groupby('job_id')[cost_col].sum().reset_index()
        job_costs = job_costs.nlargest(25, cost_col)
        
        # Create horizontal bar chart for top jobs by growth/cost
        chart = create_horizontal_bar_chart(
            job_costs, 'job_id', cost_col,
            'Top 25 Jobs by Total Cost (Growth Analysis)', DATABRICKS_COLORS[0]
        )
        st.altair_chart(chart, use_container_width=True)
    
    # Show trend line chart if possible
    if len(trend_data) > 1:
        # Aggregate by date for trend visualization
        daily_costs = trend_data.groupby(date_col)[cost_col].sum().reset_index()
        
        chart = create_line_chart(
            daily_costs, date_col, cost_col,
            'Job Spend Trend Over Time', DATABRICKS_COLORS[1]
        )
        st.altair_chart(chart, use_container_width=True)
    
    # Data table
    create_data_table(trend_data, "📊 Job Spend Trend Data")

def show_failed_jobs_analysis():
    """Show failed jobs analysis"""
    st.subheader("❌ Failed Jobs Analysis")
    
    # Get data source configuration from session state
    config = st.session_state.get('data_source_config', {'http_path': None})
    
    # Load data
    failed_data = load_data('failed_jobs_analysis',
                           http_path=config['http_path'])
    if failed_data.empty:
        st.warning("No failed jobs analysis data available")
        return
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_failures = len(failed_data)
        create_metric_card("Total Failed Jobs", f"{total_failures:,}")
    
    with col2:
        if 'effective_cost' in failed_data.columns:
            total_failure_cost = failed_data['effective_cost'].sum()
            create_metric_card("Total Failure Cost", format_currency(total_failure_cost))
    
    with col3:
        if 'job_id' in failed_data.columns:
            unique_failed_jobs = failed_data['job_id'].nunique()
            create_metric_card("Unique Failed Jobs", f"{unique_failed_jobs:,}")
    
    with col4:
        if 'effective_cost' in failed_data.columns:
            avg_failure_cost = failed_data['effective_cost'].mean()
            create_metric_card("Avg Cost per Failure", format_currency(avg_failure_cost))
    
    # Charts section
    if 'job_id' in failed_data.columns:
        # Jobs with most failures
        failure_counts = failed_data['job_id'].value_counts().reset_index()
        failure_counts.columns = ['job_id', 'failure_count']
        failure_counts = failure_counts.head(25)
        
        chart = alt.Chart(failure_counts).mark_bar(
            color=DATABRICKS_COLORS[0]
        ).encode(
            x=alt.X('failure_count:Q', title='Number of Failures'),
            y=alt.Y('job_id:N', title='Job ID', sort='-x'),
            tooltip=[
                alt.Tooltip('job_id:N', title='Job ID'),
                alt.Tooltip('failure_count:Q', title='Failure Count')
            ]
        ).properties(
            title='Top 25 Jobs with Most Failures',
            width=600,
            height=400
        )
        
        st.altair_chart(chart, use_container_width=True)
    
    # Jobs with highest failure costs
    if 'effective_cost' in failed_data.columns and 'job_id' in failed_data.columns:
        job_failure_costs = failed_data.groupby('job_id')['effective_cost'].sum().reset_index()
        job_failure_costs = job_failure_costs.nlargest(25, 'effective_cost')
        
        chart = create_horizontal_bar_chart(
            job_failure_costs, 'job_id', 'effective_cost',
            'Top 25 Jobs by Failure Costs', DATABRICKS_COLORS[1]
        )
        st.altair_chart(chart, use_container_width=True)
    
    # Data table
    create_data_table(failed_data, "❌ Failed Jobs Data")

if __name__ == "__main__":
    show_job_analytics()