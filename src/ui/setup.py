#!/usr/bin/env python3
"""
Databricks IQ Setup Script
Automated setup for the cost management dashboard
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

def print_banner():
    """Print the setup banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                        Databricks IQ                         ║
    ║                 Cost Management Dashboard                     ║
    ║                                                              ║
    ║                 🧱 Setup & Configuration                      ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8+ is required")
        print(f"   Current version: {version.major}.{version.minor}.{version.micro}")
        return False
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True

def install_dependencies():
    """Install required Python packages"""
    print("\n📦 Installing dependencies...")
    
    ui_dir = Path(__file__).parent
    requirements_file = ui_dir / "requirements.txt"
    
    if not requirements_file.exists():
        print("❌ requirements.txt not found")
        return False
    
    try:
        cmd = [sys.executable, "-m", "pip", "install", "-r", str(requirements_file)]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        print(f"   Error output: {e.stderr}")
        return False

def check_databricks_cli():
    """Check if Databricks CLI is available"""
    print("\n🔧 Checking Databricks CLI...")
    try:
        result = subprocess.run(["databricks", "--version"], 
                              capture_output=True, text=True, check=True)
        print("✅ Databricks CLI is available")
        print(f"   Version: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("⚠️  Databricks CLI not found (optional for live data)")
        print("   Install with: pip install databricks-cli")
        return False

def setup_environment():
    """Setup environment configuration"""
    print("\n🌍 Environment Configuration")
    
    # Check for existing environment variables
    databricks_vars = {
        "DATABRICKS_HOST": "Your Databricks workspace URL",
        "DATABRICKS_TOKEN": "Your access token",
        "SERVING_ENDPOINT": "Your MCP agent endpoint (optional)"
    }
    
    print("\nCurrent environment variables:")
    for var, description in databricks_vars.items():
        value = os.getenv(var)
        if value:
            # Mask token for security
            if "TOKEN" in var:
                masked_value = value[:8] + "..." if len(value) > 8 else "***"
                print(f"   ✅ {var}: {masked_value}")
            else:
                print(f"   ✅ {var}: {value}")
        else:
            print(f"   ⚪ {var}: Not set ({description})")
    
    print("\nFor live Databricks data, configure authentication using:")
    print("   1. Environment variables (shown above)")
    print("   2. Databricks CLI: databricks configure")
    print("   3. Databricks configuration file ~/.databricks/cfg")

def check_example_data():
    """Check if example data is available"""
    print("\n📊 Checking example data...")
    
    ui_dir = Path(__file__).parent
    example_data_dir = ui_dir / "example_data"
    
    if not example_data_dir.exists():
        print("❌ Example data directory not found")
        print(f"   Expected: {example_data_dir}")
        return False
    
    expected_files = [
        "job_spend_trend.csv",
        "serverless_job_spend.csv", 
        "model_serving_costs.csv",
        "user_serverless_consumption.csv"
    ]
    
    missing_files = []
    for file in expected_files:
        if not (example_data_dir / file).exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"⚠️  Missing example data files: {', '.join(missing_files)}")
        return False
    else:
        print(f"✅ Example data available ({len(expected_files)} files)")
        return True

def test_imports():
    """Test if all required packages can be imported"""
    print("\n🧪 Testing package imports...")
    
    packages = [
        ("streamlit", "Streamlit web framework"),
        ("pandas", "Data manipulation library"),
        ("plotly", "Interactive visualization library"),
        ("databricks.sql", "Databricks SQL connector"),
        ("databricks.sdk", "Databricks SDK")
    ]
    
    success_count = 0
    for package, description in packages:
        try:
            __import__(package)
            print(f"   ✅ {package} - {description}")
            success_count += 1
        except ImportError:
            print(f"   ❌ {package} - {description} (import failed)")
    
    if success_count == len(packages):
        print("✅ All packages imported successfully")
        return True
    else:
        print(f"⚠️  {len(packages) - success_count} packages failed to import")
        return False

def create_run_script():
    """Create platform-specific run scripts"""
    print("\n📝 Creating run scripts...")
    
    ui_dir = Path(__file__).parent
    
    # Create shell script for Unix-like systems
    shell_script = ui_dir / "start.sh"
    shell_content = """#!/bin/bash
echo "🧱 Starting Databricks IQ Dashboard..."
cd "$(dirname "$0")"
streamlit run app.py
"""
    
    try:
        shell_script.write_text(shell_content)
        shell_script.chmod(0o755)  # Make executable
        print("   ✅ Created start.sh (Unix/Linux/macOS)")
    except Exception as e:
        print(f"   ⚠️  Could not create shell script: {e}")
    
    # Create batch script for Windows
    if platform.system() == "Windows":
        batch_script = ui_dir / "start.bat"
        batch_content = """@echo off
echo 🧱 Starting Databricks IQ Dashboard...
cd /d "%~dp0"
streamlit run app.py
pause
"""
        try:
            batch_script.write_text(batch_content)
            print("   ✅ Created start.bat (Windows)")
        except Exception as e:
            print(f"   ⚠️  Could not create batch script: {e}")

def show_next_steps():
    """Show next steps to the user"""
    print("\n" + "="*60)
    print("🎉 Setup Complete! Next Steps:")
    print("="*60)
    
    print("\n1. 🚀 Start the application:")
    print("   streamlit run app.py")
    
    print("\n2. 🌐 Access the dashboard:")
    print("   http://localhost:8501")
    
    print("\n3. 📊 Choose your data source:")
    print("   • Example Data: Use provided CSV files for demonstration")
    print("   • Live Databricks Data: Connect to your workspace")
    
    print("\n4. 🏷️ Configure the chatbot (optional):")
    print("   • Ensure MCP server is running")
    print("   • Set SERVING_ENDPOINT environment variable")
    print("   • Configure Databricks authentication")
    
    print("\n5. 📚 Explore the features:")
    print("   • Overview: Cost trends and key metrics")
    print("   • Job Analytics: Detailed job spend analysis")
    print("   • Serverless Analytics: Serverless compute insights") 
    print("   • Model Serving: ML model costs and usage")
    print("   • Resource Assistant: AI-powered tag management")

def main():
    """Main setup function"""
    print_banner()
    
    # Run all checks
    checks = [
        ("Python version", check_python_version),
        ("Dependencies", install_dependencies),
        ("Package imports", test_imports),
        ("Example data", check_example_data)
    ]
    
    passed = 0
    total = len(checks)
    
    for name, check_func in checks:
        print(f"\n{'='*20} {name} {'='*20}")
        if check_func():
            passed += 1
        else:
            print(f"❌ {name} check failed")
    
    # Optional checks
    check_databricks_cli()
    setup_environment()
    create_run_script()
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Setup Summary: {passed}/{total} required checks passed")
    print("="*60)
    
    if passed == total:
        print("🎉 Setup completed successfully!")
        show_next_steps()
    else:
        print("⚠️  Some checks failed. Please resolve issues before running.")
        print("   Check the output above for specific error messages.")
        sys.exit(1)

if __name__ == "__main__":
    main()