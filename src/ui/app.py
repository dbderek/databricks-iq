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
from chatbot import show_chatbot

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
        logo_path = Path(__file__).parent / "DatabricksLogo.png"
        if logo_path.exists():
            return str(logo_path)
    except:
        pass
    return None

def show_header():
    """Display the Databricks IQ header"""
    logo_path = load_databricks_logo()
    
    st.markdown(DATABRICKS_CSS, unsafe_allow_html=True)
    
    if logo_path:
        st.image(logo_path, width=120)
    
    header_html = f"""
    <div class="databricks-header">
        <div class="databricks-logo">
            <div>
                <h1 class="databricks-title">Databricks IQ</h1>
                <p class="databricks-subtitle">Cost Management & Analytics Dashboard</p>
            </div>
        </div>
    </div>
    """
    
    st.markdown(header_html, unsafe_allow_html=True)

def show_chatbot():
    """Show the chatbot interface - moved to separate file"""
    from chatbot import show_chatbot as chatbot_interface
    chatbot_interface()

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