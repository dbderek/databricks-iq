"""
Databricks IQ - Cost Management Dashboard
A comprehensive Streamlit application for Databricks cost analysis and resource management
"""

import streamlit as st
import pandas as pd
import altair as alt
import warnings
from pathlib import Path
import sys
import os

# Suppress all warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Configure Altair
alt.data_transformers.disable_max_rows()

# Set environment variable to suppress warnings
os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"

# Suppress logging
import logging
logging.getLogger("streamlit").setLevel(logging.ERROR)

# Add the parent directory to the path so we can import the agent
sys.path.append(str(Path(__file__).parent.parent))

# Import the page modules
from job_analytics import show_job_analytics
from serverless_analytics import show_serverless_analytics
from model_serving_analytics import show_model_serving_analytics
from user_analytics import show_user_analytics

# Custom CSS for Databricks styling
DATABRICKS_CSS = """
<style>
/* Import Databricks-inspired fonts */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* Global styles */
.main {
    background-color: #FAFAFA;
    font-family: 'Inter', sans-serif;
}

/* Header styling */
.databricks-header {
    background: linear-gradient(90deg, #FF3621 0%, #FF8A00 100%);
    padding: 1rem 2rem;
    margin: -1rem -1rem 2rem -1rem;
    color: white;
    border-radius: 0 0 15px 15px;
}

.databricks-logo {
    display: flex;
    align-items: center;
    margin-bottom: 1rem;
}

.databricks-logo img {
    height: 30px;
    margin-right: 1rem;
}

.databricks-title {
    font-size: 2rem;
    font-weight: 700;
    margin: 0;
    color: white;
}

.databricks-subtitle {
    font-size: 1.1rem;
    font-weight: 400;
    opacity: 0.9;
    margin: 0;
}

/* Navigation styling */
.nav-container {
    background: white;
    padding: 1rem;
    border-radius: 10px;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
    margin-bottom: 2rem;
}

/* Metric cards */
.metric-card {
    background: white;
    padding: 1.5rem;
    border-radius: 10px;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
    border-left: 4px solid #FF3621;
    margin-bottom: 1rem;
}

.metric-title {
    font-size: 0.9rem;
    font-weight: 600;
    color: #1B3139;
    margin-bottom: 0.5rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.metric-value {
    font-size: 2rem;
    font-weight: 700;
    color: #FF3621;
    margin: 0;
}

.metric-delta {
    font-size: 0.8rem;
    font-weight: 500;
    color: #00A1F1;
    margin-top: 0.5rem;
}

/* Section headers */
.section-header {
    font-size: 1.5rem;
    font-weight: 600;
    color: #1B3139;
    margin: 2rem 0 1rem 0;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid #FF3621;
}

/* Data tables */
.dataframe {
    border-radius: 10px !important;
    overflow: hidden !important;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05) !important;
}

/* Charts */
.vega-embed {
    border-radius: 10px;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
}

/* Sidebar */
.css-1d391kg {
    background-color: #F8F9FA;
}

/* Buttons */
.stButton > button {
    background: linear-gradient(90deg, #FF3621 0%, #FF8A00 100%);
    color: white;
    border: none;
    border-radius: 5px;
    padding: 0.5rem 1rem;
    font-weight: 600;
    transition: all 0.3s ease;
}

.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 15px rgba(255, 54, 33, 0.3);
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    gap: 2rem;
}

.stTabs [data-baseweb="tab"] {
    background-color: #F8F9FA;
    border-radius: 5px 5px 0 0;
    padding: 1rem 2rem;
    font-weight: 600;
    color: #1B3139;
    border: 2px solid #E5E5E5;
    border-bottom: none;
}

.stTabs [aria-selected="true"] {
    background-color: white;
    border-color: #FF3621;
    color: #FF3621;
}

/* Fix scrollbar */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: #F1F1F1;
    border-radius: 10px;
}

::-webkit-scrollbar-thumb {
    background: #FF3621;
    border-radius: 10px;
}

::-webkit-scrollbar-thumb:hover {
    background: #E12E1C;
}
</style>
"""

def load_databricks_logo():
    """Load Databricks logo if available"""
    try:
        logo_path = Path(__file__).parent.parent.parent / "DatabricksLogo.png"
        if logo_path.exists():
            return str(logo_path)
    except:
        pass
    return None

def show_header():
    """Display the Databricks IQ header"""
    logo_path = load_databricks_logo()
    
    st.markdown(DATABRICKS_CSS, unsafe_allow_html=True)
    
    header_html = f"""
    <div class="databricks-header">
        <div class="databricks-logo">
            {f'<img src="data:image/png;base64,{logo_path}" alt="Databricks">' if logo_path else '🧱'}
            <div>
                <h1 class="databricks-title">Databricks IQ</h1>
                <p class="databricks-subtitle">Cost Management & Analytics Dashboard</p>
            </div>
        </div>
    </div>
    """
    
    st.markdown(header_html, unsafe_allow_html=True)

def show_chatbot():
    """Show the chatbot interface"""
    st.header("🤖 Databricks IQ Assistant")
    st.markdown("Chat with your intelligent assistant to get insights about your Databricks costs")
    
    try:
        # Try to import the agent
        from agent.agent import DatabricksIQAgent
        
        # Initialize chat history
        if "messages" not in st.session_state:
            st.session_state.messages = []
            st.session_state.messages.append({
                "role": "assistant",
                "content": "Hello! I'm your Databricks IQ assistant. I can help you analyze costs, understand spending patterns, and provide insights about your Databricks usage. What would you like to know?"
            })
        
        # Display chat messages from history on app rerun
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        
        # Accept user input
        if prompt := st.chat_input("Ask me about your Databricks costs..."):
            # Add user message to chat history
            st.session_state.messages.append({"role": "user", "content": prompt})
            
            # Display user message in chat message container
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # Get response from agent
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        # Initialize the agent (you may need to adjust this based on your agent implementation)
                        agent = DatabricksIQAgent()
                        response = agent.process_query(prompt)
                        
                        st.markdown(response)
                        
                        # Add assistant response to chat history
                        st.session_state.messages.append({"role": "assistant", "content": response})
                    
                    except Exception as e:
                        error_message = f"Sorry, I encountered an error: {str(e)}"
                        st.error(error_message)
                        st.session_state.messages.append({"role": "assistant", "content": error_message})
    
    except ImportError:
        st.warning("Chatbot agent is not available. Please check the agent implementation in `/src/agent/`.")
        st.info("You can still use the analytics pages to explore your Databricks costs.")

def main():
    """Main application function"""
    # Configure page
    st.set_page_config(
        page_title="Databricks IQ - Cost Management Dashboard",
        page_icon="🧱",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Show header
    show_header()
    
    # Navigation
    st.sidebar.title("📊 Navigation")
    
    # Create navigation menu
    pages = {
        "💼 Job Analytics": "job_analytics",
        "⚡ Serverless Analytics": "serverless_analytics", 
        "🤖 Model Serving Analytics": "model_serving_analytics",
        "👥 User Analytics": "user_analytics",
        "🤖 AI Assistant": "chatbot"
    }
    
    selected_page = st.sidebar.selectbox(
        "Select a page:",
        list(pages.keys()),
        index=0
    )
    
    # Add some info in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📈 Quick Stats")
    st.sidebar.info("Navigate between different analytics pages to explore your Databricks costs and usage patterns.")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔧 Settings")
    
    # Data source toggle (for future implementation)
    data_source = st.sidebar.selectbox(
        "Data Source",
        ["Example Data", "Live Databricks SQL"],
        help="Switch between example data and live Databricks SQL queries"
    )
    
    if data_source == "Live Databricks SQL":
        st.sidebar.warning("Live Databricks SQL integration is not yet implemented. Using example data.")
    
    # Show selected page
    page_key = pages[selected_page]
    
    if page_key == "job_analytics":
        show_job_analytics()
    elif page_key == "serverless_analytics":
        show_serverless_analytics()
    elif page_key == "model_serving_analytics":
        show_model_serving_analytics()
    elif page_key == "user_analytics":
        show_user_analytics()
    elif page_key == "chatbot":
        show_chatbot()

if __name__ == "__main__":
    main()
}

.stApp {
    font-family: 'Inter', sans-serif;
}

/* Databricks brand colors */
:root {
    --databricks-orange: #FF3621;
    --databricks-blue: #00A1F1;
    --databricks-dark: #1B3139;
    --databricks-gray: #9E9E9E;
    --databricks-light-gray: #F5F7FA;
}

/* Header styling */
.main-header {
    color: var(--databricks-dark);
    font-size: 3.2rem;
    font-weight: 700;
    text-align: center;
    margin-bottom: 2rem;
    padding: 2rem 0;
    background: linear-gradient(135deg, #FF3621, #FF6B47);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.page-header {
    color: var(--databricks-dark);
    font-size: 2.4rem;
    font-weight: 600;
    margin-bottom: 1.5rem;
    border-bottom: 3px solid var(--databricks-orange);
    padding-bottom: 0.5rem;
}

.section-header {
    color: var(--databricks-dark);
    font-size: 1.6rem;
    font-weight: 500;
    margin: 1.5rem 0 1rem 0;
    padding-left: 1rem;
    border-left: 4px solid var(--databricks-blue);
}

/* Metric cards */
.metric-card {
    background: white;
    border-radius: 12px;
    padding: 1.5rem;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    border-left: 4px solid var(--databricks-orange);
    margin-bottom: 1rem;
}

.metric-value {
    font-size: 2.4rem;
    font-weight: 700;
    color: var(--databricks-dark);
}

.metric-label {
    font-size: 0.9rem;
    color: var(--databricks-gray);
    text-transform: uppercase;
    font-weight: 500;
    letter-spacing: 0.5px;
}

/* Filter cards */
.filter-section {
    background: white;
    border-radius: 8px;
    padding: 1.5rem;
    margin-bottom: 2rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    border: 1px solid #E0E0E0;
}

/* Chart containers */
.chart-container {
    background: white;
    border-radius: 12px;
    padding: 2rem;
    margin: 2rem 0;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    border: 1px solid #F0F0F0;
}

/* Status indicators */
.status-success {
    color: #4CAF50;
    font-weight: 600;
}

.status-warning {
    color: #FF9800;
    font-weight: 600;
}

.status-error {
    color: #F44336;
    font-weight: 600;
}

/* Sidebar styling */
.css-1d391kg {
    background-color: var(--databricks-light-gray);
}

/* Button styling */
.stButton > button {
    background: linear-gradient(135deg, var(--databricks-orange), #FF6B47);
    color: white;
    border: none;
    border-radius: 6px;
    padding: 0.5rem 2rem;
    font-weight: 500;
    transition: all 0.3s ease;
}

.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(255, 54, 33, 0.3);
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] button {
    font-weight: 500;
    color: var(--databricks-dark);
}

.stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
    color: var(--databricks-orange);
    border-bottom-color: var(--databricks-orange);
}

/* Table styling */
.stDataFrame {
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}

/* Alert styling */
.alert-info {
    background-color: #E3F2FD;
    border: 1px solid var(--databricks-blue);
    border-radius: 8px;
    padding: 1rem;
    margin: 1rem 0;
}

/* Logo container */
.logo-container {
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 1rem 0;
}

/* Hide streamlit branding */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Custom scrollbar */
::-webkit-scrollbar {
    width: 8px;
}

::-webkit-scrollbar-track {
    background: #f1f1f1;
}

::-webkit-scrollbar-thumb {
    background: var(--databricks-orange);
    border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
    background: #e52e1a;
}
</style>
"""

# Standard plotly config to prevent deprecation warnings
# Removed - using Altair instead

def load_logo():
    """Load and display the Databricks logo"""
    logo_path = Path(__file__).parent / "DatabricksLogo.png"
    if logo_path.exists():
        return str(logo_path)
    return None

@st.cache_data
def load_example_data(filename):
    """Load example data from CSV files"""
    try:
        data_path = Path(__file__).parent / "example_data" / filename
        if data_path.exists():
            return pd.read_csv(data_path)
        else:
            st.error(f"Data file not found: {filename}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading {filename}: {str(e)}")
        return pd.DataFrame()

@st.cache_resource
def get_databricks_connection(http_path):
    """Create connection to Databricks SQL warehouse"""
    try:
        cfg = Config()
        return sql.connect(
            server_hostname=cfg.host,
            http_path=http_path,
            credentials_provider=lambda: cfg.authenticate,
        )
    except Exception as e:
        st.error(f"Failed to connect to Databricks: {str(e)}")
        return None

def read_databricks_table(table_name, conn):
    """Read data from Databricks table"""
    try:
        with conn.cursor() as cursor:
            query = f"SELECT * FROM {table_name} LIMIT 1000"
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        st.error(f"Error reading table {table_name}: {str(e)}")
        return pd.DataFrame()

def format_currency(value):
    """Format currency values"""
    if pd.isna(value):
        return "$0"
    return f"${value:,.2f}"

def format_percentage(value):
    """Format percentage values"""
    if pd.isna(value):
        return "0%"
    return f"{value:.1f}%"

def parse_tags(tag_string):
    """Parse JSON tag string into dictionary"""
    try:
        if pd.isna(tag_string) or tag_string == "":
            return {}
        # Clean up the tag string - handle escaped quotes
        cleaned = tag_string.replace('""', '"')
        return json.loads(cleaned)
    except:
        return {}

def create_metric_card(title, value, delta=None, delta_color="normal"):
    """Create a styled metric card"""
    delta_html = ""
    if delta is not None:
        color = "green" if delta_color == "normal" and delta > 0 else "red" if delta_color == "inverse" and delta > 0 else "green"
        arrow = "↑" if delta > 0 else "↓" if delta < 0 else "→"
        delta_html = f'<div style="color: {color}; font-size: 1rem; margin-top: 0.5rem;">{arrow} {abs(delta):.1f}%</div>'
    
    return f"""
    <div class="metric-card">
        <div class="metric-label">{title}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """

def show_overview():
    """Display the overview dashboard"""
    st.markdown('<div class="page-header">📊 Overview Dashboard</div>', unsafe_allow_html=True)
    
    # Load key datasets
    job_spend = load_example_data("job_spend_trend.csv")
    serverless_spend = load_example_data("serverless_job_spend.csv")
    model_costs = load_example_data("model_serving_costs.csv")
    
    if not job_spend.empty and not serverless_spend.empty and not model_costs.empty:
        # Calculate key metrics
        total_job_spend = job_spend['Last7DaySpend'].sum()
        total_serverless_spend = serverless_spend['effective_cost'].sum()
        total_model_costs = model_costs['total_effective_cost'].sum()
        total_spend = total_job_spend + total_serverless_spend + total_model_costs
        
        growth_rate = job_spend['Last7DayGrowthPct'].mean()
        
        # Display metrics in columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(create_metric_card(
                "Total Spend (7d)", 
                format_currency(total_spend)
            ), unsafe_allow_html=True)
        
        with col2:
            st.markdown(create_metric_card(
                "Job Spend", 
                format_currency(total_job_spend)
            ), unsafe_allow_html=True)
        
        with col3:
            st.markdown(create_metric_card(
                "Model Serving", 
                format_currency(total_model_costs)
            ), unsafe_allow_html=True)
        
        with col4:
            st.markdown(create_metric_card(
                "Growth Rate", 
                format_percentage(growth_rate),
                growth_rate,
                "inverse"
            ), unsafe_allow_html=True)
        
        # Spend breakdown chart
        st.markdown('<div class="section-header">💰 Spend Breakdown</div>', unsafe_allow_html=True)
        
        spend_data = pd.DataFrame({
            'Category': ['Jobs', 'Serverless', 'Model Serving'],
            'Amount': [total_job_spend, total_serverless_spend, total_model_costs]
        })
        
        # Create Altair pie chart
        chart = alt.Chart(spend_data).add_selection(
            alt.selection_single()
        ).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="Amount", type="quantitative"),
            color=alt.Color(field="Category", type="nominal", 
                          scale=alt.Scale(range=['#FF3621', '#00A1F1', '#7C4DFF'])),
            tooltip=['Category', alt.Tooltip('Amount:Q', format='$.2f')]
        ).resolve_scale(
            color='independent'
        ).properties(
            title="7-Day Spend by Category",
            width=400,
            height=400
        )
        
        st.altair_chart(chart, use_container_width=True)
        
        # Top spenders
        st.markdown('<div class="section-header">🏆 Top Spending Jobs</div>', unsafe_allow_html=True)
        
        top_jobs = job_spend.nlargest(10, 'Last7DaySpend')[['name', 'Last7DaySpend', 'run_as']].copy()
        top_jobs['Last7DaySpend'] = top_jobs['Last7DaySpend'].apply(format_currency)
        top_jobs.columns = ['Job Name', '7-Day Spend', 'Owner']
        
        st.dataframe(top_jobs, width="stretch", hide_index=True)

def show_job_metrics():
    """Display job spend metrics with filters"""
    st.markdown('<div class="page-header">🛠️ Job Spend Analytics</div>', unsafe_allow_html=True)
    
    # Load job data
    job_data = load_example_data("job_spend_trend.csv")
    job_alerts = load_example_data("job_spend_alerts.csv")
    
    if job_data.empty:
        st.warning("No job data available")
        return
    
    # Parse tags for filtering
    job_data['parsed_tags'] = job_data['custom_tags'].apply(parse_tags)
    
    # Create filters in sidebar
    st.sidebar.markdown('<div class="section-header">🔍 Job Filters</div>', unsafe_allow_html=True)
    
    # Workspace filter
    workspaces = sorted(job_data['workspace_id'].dropna().unique())
    selected_workspaces = st.sidebar.multiselect(
        "Workspaces",
        options=workspaces,
        default=workspaces[:3]  # Select first 3 by default
    )
    
    # Owner filter
    owners = sorted(job_data['run_as'].dropna().unique())
    selected_owners = st.sidebar.multiselect(
        "Owners (run_as)",
        options=owners,
        default=[]
    )
    
    # Tag filters - extract common tag keys
    all_tags = {}
    for tags in job_data['parsed_tags']:
        all_tags.update(tags)
    
    common_tag_keys = ['owner', 'Owner', 'environment', 'Environment', 'team', 'project', 'budget']
    available_tag_keys = [key for key in common_tag_keys if key in all_tags.keys()]
    
    if available_tag_keys:
        selected_tag_key = st.sidebar.selectbox("Filter by Tag", ["None"] + available_tag_keys)
        if selected_tag_key != "None":
            # Get unique values for selected tag
            tag_values = set()
            for tags in job_data['parsed_tags']:
                if selected_tag_key in tags:
                    tag_values.add(tags[selected_tag_key])
            tag_values = sorted(list(tag_values))
            selected_tag_values = st.sidebar.multiselect(f"{selected_tag_key} Values", tag_values)
    else:
        selected_tag_key = "None"
        selected_tag_values = []
    
    # Apply filters
    filtered_data = job_data.copy()
    
    if selected_workspaces:
        filtered_data = filtered_data[filtered_data['workspace_id'].isin(selected_workspaces)]
    
    if selected_owners:
        filtered_data = filtered_data[filtered_data['run_as'].isin(selected_owners)]
    
    if selected_tag_key != "None" and selected_tag_values:
        mask = filtered_data['parsed_tags'].apply(
            lambda tags: tags.get(selected_tag_key) in selected_tag_values
        )
        filtered_data = filtered_data[mask]
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_7d = filtered_data['Last7DaySpend'].sum()
        st.markdown(create_metric_card(
            "7-Day Spend", 
            format_currency(total_7d)
        ), unsafe_allow_html=True)
    
    with col2:
        total_14d = filtered_data['Last14DaySpend'].sum()
        st.markdown(create_metric_card(
            "14-Day Spend", 
            format_currency(total_14d)
        ), unsafe_allow_html=True)
    
    with col3:
        avg_growth = filtered_data['Last7DayGrowthPct'].mean()
        st.markdown(create_metric_card(
            "Avg Growth", 
            format_percentage(avg_growth),
            avg_growth,
            "inverse"
        ), unsafe_allow_html=True)
    
    with col4:
        job_count = len(filtered_data)
        st.markdown(create_metric_card(
            "Jobs", 
            str(job_count)
        ), unsafe_allow_html=True)
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="section-header">📈 Spend Trend</div>', unsafe_allow_html=True)
        
        # Create spend comparison chart
        spend_comparison = pd.DataFrame({
            'Period': ['14-Day', '7-Day'],
            'Spend': [total_14d, total_7d]
        })
        
        chart = alt.Chart(spend_comparison).mark_bar(
            color='#FF3621'
        ).encode(
            x=alt.X('Period:N', title='Period'),
            y=alt.Y('Spend:Q', title='Spend ($)', axis=alt.Axis(format='$.0f')),
            tooltip=[alt.Tooltip('Spend:Q', format='$.2f')]
        ).properties(
            title="Spend Comparison",
            width=300,
            height=300
        )
        
        st.altair_chart(chart, use_container_width=True)
    
    with col2:
        st.markdown('<div class="section-header">📊 Growth Distribution</div>', unsafe_allow_html=True)
        
        # Growth rate histogram
        chart = alt.Chart(filtered_data).mark_bar(
            color='#00A1F1',
            opacity=0.7
        ).encode(
            x=alt.X('Last7DayGrowthPct:Q', 
                   title='7-Day Growth (%)', 
                   bin=alt.Bin(maxbins=20)),
            y=alt.Y('count()', title='Frequency'),
            tooltip=[alt.Tooltip('count()', title='Count')]
        ).properties(
            title="Distribution of 7-Day Growth Rates",
            width=400,
            height=300
        )
        
        st.altair_chart(chart, use_container_width=True)    # Top spenders table
    st.markdown('<div class="section-header">🏆 Top Spenders</div>', unsafe_allow_html=True)
    
    top_spenders = filtered_data.nlargest(20, 'Last7DaySpend')[[
        'name', 'workspace_id', 'run_as', 'Last7DaySpend', 'Last7DayGrowthPct', 'sku_name'
    ]].copy()
    
    top_spenders['Last7DaySpend'] = top_spenders['Last7DaySpend'].apply(format_currency)
    top_spenders['Last7DayGrowthPct'] = top_spenders['Last7DayGrowthPct'].apply(format_percentage)
    top_spenders.columns = ['Job Name', 'Workspace ID', 'Owner', '7-Day Spend', 'Growth %', 'SKU']
    
    st.dataframe(top_spenders, width="stretch", hide_index=True)

def show_serverless_metrics():
    """Display serverless spend metrics"""
    st.markdown('<div class="page-header">⚡ Serverless Analytics</div>', unsafe_allow_html=True)
    
    # Load serverless data
    serverless_data = load_example_data("serverless_job_spend.csv")
    notebook_spend = load_example_data("serverless_notebook_spend.csv")
    user_consumption = load_example_data("user_serverless_consumption.csv")
    
    if serverless_data.empty:
        st.warning("No serverless data available")
        return
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_cost = serverless_data['effective_cost'].sum()
        st.markdown(create_metric_card(
            "Total Cost", 
            format_currency(total_cost)
        ), unsafe_allow_html=True)
    
    with col2:
        total_dbu = serverless_data['total_dbu'].sum()
        st.markdown(create_metric_card(
            "Total DBUs", 
            f"{total_dbu:,.0f}"
        ), unsafe_allow_html=True)
    
    with col3:
        job_count = len(serverless_data)
        st.markdown(create_metric_card(
            "Serverless Jobs", 
            str(job_count)
        ), unsafe_allow_html=True)
    
    with col4:
        avg_cost_per_dbu = total_cost / total_dbu if total_dbu > 0 else 0
        st.markdown(create_metric_card(
            "Avg Cost/DBU", 
            format_currency(avg_cost_per_dbu)
        ), unsafe_allow_html=True)
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="section-header">💰 Cost Distribution</div>', unsafe_allow_html=True)
        
        # Top 10 jobs by cost
        top_jobs = serverless_data.nlargest(10, 'effective_cost')
        
        chart = alt.Chart(top_jobs).mark_bar(
            color='#FF3621'
        ).encode(
            x=alt.X('effective_cost:Q', 
                   title='Effective Cost ($)', 
                   axis=alt.Axis(format='$.0f')),
            y=alt.Y('job_name:N', 
                   title='Job Name',
                   sort=alt.EncodingSortField(field='effective_cost', order='descending')),
            tooltip=[
                alt.Tooltip('job_name:N', title='Job Name'),
                alt.Tooltip('effective_cost:Q', title='Cost', format='$.2f')
            ]
        ).properties(
            title="Top 10 Jobs by Cost",
            width=500,
            height=500
        )
        
        st.altair_chart(chart, use_container_width=True)
    
    with col2:
        st.markdown('<div class="section-header">⚡ DBU Usage</div>', unsafe_allow_html=True)
        
        # DBU vs Cost scatter plot
        chart = alt.Chart(serverless_data).mark_circle(
            color='#00A1F1',
            size=60
        ).encode(
            x=alt.X('total_dbu:Q', title='Total DBU'),
            y=alt.Y('effective_cost:Q', title='Effective Cost ($)', axis=alt.Axis(format='$.0f')),
            tooltip=[
                alt.Tooltip('job_name:N', title='Job Name'),
                alt.Tooltip('total_dbu:Q', title='Total DBU'),
                alt.Tooltip('effective_cost:Q', title='Cost', format='$.2f')
            ]
        ).properties(
            title="DBU Usage vs Cost",
            width=400,
            height=350
        )
        
        st.altair_chart(chart, use_container_width=True)
    
    # User consumption analysis
    if not user_consumption.empty:
        st.markdown('<div class="section-header">👤 User Consumption</div>', unsafe_allow_html=True)
        
        # Group by user and aggregate the data
        user_summary = user_consumption.groupby('run_as').agg({
            'effective_cost': 'sum',
            'list_cost': 'sum',
            'total_dbu': 'sum',
            'job_id': 'nunique'  # Count unique jobs per user
        }).reset_index()
        
        # Rename columns for display
        user_summary.columns = ['user_name', 'total_effective_cost', 'total_list_cost', 'total_dbu', 'job_count']
        
        # Top users by consumption
        top_users = user_summary.nlargest(15, 'total_effective_cost')[[
            'user_name', 'total_effective_cost', 'total_list_cost', 'job_count'
        ]].copy()
        
        top_users['total_effective_cost'] = top_users['total_effective_cost'].apply(format_currency)
        top_users['total_list_cost'] = top_users['total_list_cost'].apply(format_currency)
        top_users.columns = ['User', 'Effective Cost', 'List Cost', 'Jobs']
        
        st.dataframe(top_users, width="stretch", hide_index=True)

def show_model_serving():
    """Display model serving and batch inference costs"""
    st.markdown('<div class="page-header">🤖 Model Serving & Inference</div>', unsafe_allow_html=True)
    
    # Load model serving data
    model_data = load_example_data("model_serving_costs.csv")
    batch_data = load_example_data("batch_inference_costs.csv")
    
    if model_data.empty:
        st.warning("No model serving data available")
        return
    
    # Parse tags
    model_data['parsed_tags'] = model_data['custom_tags'].apply(parse_tags)
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_cost = model_data['total_effective_cost'].sum()
        st.markdown(create_metric_card(
            "Total Cost", 
            format_currency(total_cost)
        ), unsafe_allow_html=True)
    
    with col2:
        endpoint_count = model_data['endpoint_name'].nunique()
        st.markdown(create_metric_card(
            "Endpoints", 
            str(endpoint_count)
        ), unsafe_allow_html=True)
    
    with col3:
        t7d_cost = model_data['t7d_effective_cost'].sum()
        st.markdown(create_metric_card(
            "7-Day Cost", 
            format_currency(t7d_cost)
        ), unsafe_allow_html=True)
    
    with col4:
        foundation_models = model_data[model_data['entity_type'] == 'FOUNDATION_MODEL']['total_effective_cost'].sum()
        st.markdown(create_metric_card(
            "Foundation Models", 
            format_currency(foundation_models)
        ), unsafe_allow_html=True)
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="section-header">📊 Cost by Model Type</div>', unsafe_allow_html=True)
        
        # Group by entity type
        type_costs = model_data.groupby('entity_type')['total_effective_cost'].sum().reset_index()
        
        # Create pie chart
        base = alt.Chart(type_costs).add_selection(
            alt.selection_single()
        )
        
        chart = base.mark_arc(
            innerRadius=50,
            stroke='white',
            strokeWidth=2
        ).encode(
            theta=alt.Theta('total_effective_cost:Q'),
            color=alt.Color('entity_type:N', 
                          scale=alt.Scale(range=['#FF3621', '#00A1F1', '#7C4DFF']),
                          legend=alt.Legend(title="Entity Type")),
            tooltip=[
                alt.Tooltip('entity_type:N', title='Type'),
                alt.Tooltip('total_effective_cost:Q', title='Cost', format='$.2f')
            ]
        ).properties(
            title="Cost Distribution by Model Type",
            width=300,
            height=300
        )
        
        st.altair_chart(chart, use_container_width=True)
    
    with col2:
        st.markdown('<div class="section-header">📈 Cost Trends</div>', unsafe_allow_html=True)
        
        # Time series of costs
        trend_data = pd.DataFrame({
            'Period': ['30-Day', '14-Day', '7-Day'],
            'Cost': [
                model_data['t30d_effective_cost'].sum(),
                model_data['t14d_effective_cost'].sum(), 
                model_data['t7d_effective_cost'].sum()
            ]
        })
        
        chart = alt.Chart(trend_data).mark_line(
            color='#FF3621',
            strokeWidth=3,
            point=alt.OverlayMarkDef(
                color='#FF3621',
                size=60
            )
        ).encode(
            x=alt.X('Period:N', title='Period'),
            y=alt.Y('Cost:Q', title='Cost ($)', axis=alt.Axis(format='$.0f')),
            tooltip=[
                alt.Tooltip('Period:N', title='Period'),
                alt.Tooltip('Cost:Q', title='Cost', format='$.2f')
            ]
        ).properties(
            title="Cost Trends",
            width=400,
            height=300
        )
        
        st.altair_chart(chart, use_container_width=True)
    
    # Top endpoints table
    st.markdown('<div class="section-header">🏆 Top Endpoints by Cost</div>', unsafe_allow_html=True)
    
    top_endpoints = model_data.nlargest(15, 'total_effective_cost')[[
        'endpoint_name', 'entity_type', 'created_by', 'total_effective_cost', 't7d_effective_cost'
    ]].copy()
    
    top_endpoints['total_effective_cost'] = top_endpoints['total_effective_cost'].apply(format_currency)
    top_endpoints['t7d_effective_cost'] = top_endpoints['t7d_effective_cost'].apply(format_currency)
    top_endpoints.columns = ['Endpoint Name', 'Type', 'Created By', 'Total Cost', '7-Day Cost']
    
    st.dataframe(top_endpoints, width="stretch", hide_index=True)

def show_chatbot():
    """Display the chatbot interface"""
    st.markdown('<div class="page-header">💬 Resource Tagging Assistant</div>', unsafe_allow_html=True)
    
    try:
        # Import and show the chatbot from the agent module
        from agent.agent import AGENT
        
        st.markdown("""
        ### 🏷️ Your Databricks Resource Tagging Assistant
        
        I can help you manage tags across your Databricks resources including clusters, SQL warehouses, 
        jobs, pipelines, experiments, models, Unity Catalog resources, repositories, and serving endpoints.
        
        **Capabilities:**
        - Tag management across 12+ resource types
        - Budget policy creation and management  
        - Bulk operations and compliance reporting
        - Resource discovery and organization
        """)
        
        # Import the chatbot UI
        from ui.example_chatbot import show
        
        # Show the chatbot interface
        show({})
        
    except ImportError as e:
        st.error(f"Failed to load chatbot: {str(e)}")
        st.info("Make sure the agent module is properly configured in your environment.")
        
        # Fallback chatbot placeholder
        st.markdown("### 🚧 Chatbot Under Construction")
        st.markdown("""
        The resource tagging chatbot is being set up. It will provide:
        
        - **Tag Management**: Apply, modify, and remove tags across resources
        - **Compliance Reporting**: Generate detailed tag compliance reports  
        - **Bulk Operations**: Manage multiple resources simultaneously
        - **Budget Policies**: Create and apply budget controls
        - **Resource Discovery**: Find resources by tags and attributes
        
        Please check back soon!
        """)

def main():
    """Main application function"""
    # Set page configuration
    st.set_page_config(
        page_title="Databricks IQ - Cost Management",
        page_icon="🧱",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Apply custom CSS
    st.markdown(DATABRICKS_CSS, unsafe_allow_html=True)
    
    # Header with logo
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        logo_path = load_logo()
        if logo_path:
            st.image(logo_path, width=300)
        st.markdown('<div class="main-header">Databricks IQ</div>', unsafe_allow_html=True)
        st.markdown('<div style="text-align: center; color: #9E9E9E; margin-bottom: 2rem;">Cost Management & Resource Analytics</div>', unsafe_allow_html=True)
    
    # Navigation
    st.sidebar.markdown('<div class="section-header">📍 Navigation</div>', unsafe_allow_html=True)
    page = st.sidebar.selectbox(
        "Choose a page",
        ["Overview", "Job Analytics", "Serverless Analytics", "Model Serving", "Resource Assistant"],
        index=0
    )
    
    # Data source selection
    st.sidebar.markdown('<div class="section-header">🔌 Data Source</div>', unsafe_allow_html=True)
    data_source = st.sidebar.radio(
        "Select data source",
        ["Example Data", "Live Databricks Data"],
        index=0
    )
    
    if data_source == "Live Databricks Data":
        st.sidebar.markdown("**SQL Warehouse Configuration**")
        http_path = st.sidebar.text_input(
            "HTTP Path",
            placeholder="/sql/1.0/warehouses/your-warehouse-id",
            help="Enter your Databricks SQL Warehouse HTTP path"
        )
        
        if http_path:
            conn = get_databricks_connection(http_path)
            if conn:
                st.sidebar.success("✅ Connected to Databricks")
            else:
                st.sidebar.error("❌ Connection failed")
        else:
            st.sidebar.info("Enter HTTP path to connect")
    else:
        st.sidebar.info("Using example data for demonstration")
    
    # Show selected page
    if page == "Overview":
        show_overview()
    elif page == "Job Analytics":
        show_job_metrics()
    elif page == "Serverless Analytics":
        show_serverless_metrics()
    elif page == "Model Serving":
        show_model_serving()
    elif page == "Resource Assistant":
        show_chatbot()
    
    # Footer
    st.markdown("---")
    st.markdown(
        '<div style="text-align: center; color: #9E9E9E; padding: 2rem;">Databricks IQ - Built with ❤️ for Data Teams</div>', 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()