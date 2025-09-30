"""
Databricks LakeSpend Configuration
Customize the application settings here
"""

# Application Settings
APP_CONFIG = {
    "title": "Databricks LakeSpend",
    "subtitle": "Cost Management & Resource Analytics",
    "page_icon": "🧱",
    "layout": "wide",
    
    # Server settings for deployment
    "server": {
        "port": 8501,
        "address": "0.0.0.0"
    }
}

# Databricks Theme Configuration
THEME_CONFIG = {
    "base": "light",
    "primaryColor": "#FF3621",      # Databricks Orange
    "backgroundColor": "#FAFAFA",   # Light Gray
    "secondaryBackgroundColor": "#FFFFFF",  # White
    "textColor": "#1B3139"         # Databricks Dark
}

# Color Palette
COLORS = {
    "databricks_orange": "#FF3621",
    "databricks_blue": "#00A1F1", 
    "databricks_dark": "#1B3139",
    "databricks_gray": "#9E9E9E",
    "databricks_light_gray": "#F5F7FA",
    "success": "#4CAF50",
    "warning": "#FF9800", 
    "error": "#F44336",
    "info": "#2196F3"
}

# Chart Color Sequences
CHART_COLORS = {
    "primary": ["#FF3621", "#00A1F1", "#7C4DFF", "#4CAF50", "#FF9800"],
    "databricks": ["#FF3621", "#FF6B47", "#E52E1A", "#CC2617"],
    "blue_scale": ["#00A1F1", "#2BB0F5", "#55C0F9", "#7FCFFD"],
    "categorical": ["#FF3621", "#00A1F1", "#7C4DFF", "#4CAF50", "#FF9800", "#9C27B0", "#00BCD4"]
}

# Data Configuration
DATA_CONFIG = {
    # Databricks SQL configuration
    "databricks": {
        "catalog": "databrickslakespend",
        "schema": "main",
        "warehouse_id_env": "SQL_WAREHOUSE",  # Environment variable name
        "query_timeout": 30,
        "max_rows": 10000,
        "cache_ttl": 300  # 5 minutes
    },
    
    # Cache configuration
    "cache_ttl": 300,  # 5 minutes
    
    # Pagination
    "max_rows_display": 1000,
    "page_size": 50
}

# Feature Flags
FEATURES = {
    "live_databricks_connection": True,
    "chatbot_integration": True,
    "export_functionality": True,
    "advanced_filtering": True,
    "real_time_updates": False,
    "custom_dashboards": False
}

# Chatbot Configuration
CHATBOT_CONFIG = {
    "default_endpoint": "tag-agent",
    "max_messages": 100,
    "stream_responses": True,
    "enable_feedback": True,
    "show_examples": True
}

# Navigation Configuration
NAVIGATION = {
    "pages": [
        {"name": "Overview", "icon": "📊", "description": "High-level cost metrics and trends"},
        {"name": "Job Analytics", "icon": "🛠️", "description": "Detailed job spend analysis"},  
        {"name": "Serverless Analytics", "icon": "⚡", "description": "Serverless compute insights"},
        {"name": "Model Serving", "icon": "🤖", "description": "Model serving & inference costs"},
        {"name": "Resource Assistant", "icon": "💬", "description": "AI-powered resource management"}
    ],
    "sidebar_width": 280
}

# Metrics Configuration
METRICS_CONFIG = {
    "currency_symbol": "$",
    "currency_precision": 2,
    "percentage_precision": 1,
    "large_number_format": "abbreviated",  # "abbreviated" or "full"
    
    # Default time periods
    "time_periods": ["7d", "14d", "30d"],
    "default_period": "7d"
}

# Export Configuration  
EXPORT_CONFIG = {
    "formats": ["CSV", "Excel", "JSON"],
    "max_export_rows": 10000,
    "include_metadata": True
}

# Security Configuration
SECURITY_CONFIG = {
    "require_authentication": False,
    "allowed_domains": [],  # Empty list means all domains allowed
    "session_timeout": 3600,  # 1 hour in seconds
    "enable_audit_logging": True
}

# Performance Configuration
PERFORMANCE_CONFIG = {
    "enable_caching": True,
    "cache_backend": "memory",  # "memory", "redis", "disk"
    "max_concurrent_queries": 10,
    "query_timeout": 30,  # seconds
    "enable_query_optimization": True
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "log_to_file": False,
    "log_file": "databricks_lakespend.log"
}

# Development Configuration
DEV_CONFIG = {
    "debug_mode": False,
    "hot_reload": True,
    "show_query_times": False,
    "enable_profiling": False
}