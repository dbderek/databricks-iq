"""
Databricks IQ Chatbot Interface
A comprehensive chatbot for Databricks cost management and analytics assistance
"""

import streamlit as st
import json
import uuid
import logging
import os
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List, Dict, Any
from pathlib import Path
import sys

# Add the parent directory to the path so we can import the agent
sys.path.append(str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration - Using environment variable for serving endpoint
SERVING_ENDPOINT = os.getenv('DATABRICKS_IQ_ENDPOINT', 'dbx-iq-agent-endpoint')

# Import MLflow and Databricks SDK components
try:
    from mlflow.deployments import get_deploy_client
    from databricks.sdk import WorkspaceClient
    DEPENDENCIES_AVAILABLE = True
except ImportError:
    DEPENDENCIES_AVAILABLE = False
    logger.warning("MLflow or Databricks SDK not available. Chatbot will run in fallback mode.")

# Message classes (following the example pattern)
class Message(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def to_input_messages(self):
        """Convert this message into a list of dicts suitable for the model API."""
        pass

    @abstractmethod
    def render(self, idx):
        """Render the message in the Streamlit app."""
        pass

class UserMessage(Message):
    def __init__(self, content):
        super().__init__()
        self.content = content

    def to_input_messages(self):
        return [{
            "role": "user",
            "content": self.content
        }]

    def render(self, _):
        with st.chat_message("user"):
            st.markdown(self.content)

class AssistantResponse(Message):
    def __init__(self, messages, request_id):
        super().__init__()
        self.messages = messages
        self.request_id = request_id

    def to_input_messages(self):
        return self.messages

    def render(self, idx):
        with st.chat_message("assistant"):
            for msg in self.messages:
                render_message(msg)
            
            if self.request_id is not None:
                render_assistant_message_feedback(idx, self.request_id)

def render_message(msg):
    """Render a single message."""
    if msg["role"] == "assistant":
        if msg.get("content"):
            st.markdown(msg["content"])
        
        if "tool_calls" in msg and msg["tool_calls"]:
            for call in msg["tool_calls"]:
                fn_name = call["function"]["name"]
                args = call["function"]["arguments"]
                st.markdown(f"📊 Calling **`{fn_name}`** with:\n```json\n{args}\n```")
    elif msg["role"] == "tool":
        st.markdown("🛠️ Tool Response:")
        st.code(msg["content"], language="json")

@st.fragment
def render_assistant_message_feedback(i, request_id):
    """Render feedback UI for assistant messages."""
    def save_feedback(index):
        if SERVING_ENDPOINT and DEPENDENCIES_AVAILABLE:
            submit_feedback(
                endpoint=SERVING_ENDPOINT,
                request_id=request_id,
                rating=st.session_state[f"feedback_{index}"]
            )
    
    st.feedback("thumbs", key=f"feedback_{i}", on_change=save_feedback, args=[i])

# Model serving utilities (adapted from example)
def _get_endpoint_task_type(endpoint_name: str) -> str:
    """Get the task type of a serving endpoint."""
    if not DEPENDENCIES_AVAILABLE:
        return "chat/completions"
    
    try:
        w = WorkspaceClient()
        ep = w.serving_endpoints.get(endpoint_name)
        return ep.task if ep.task else "chat/completions"
    except Exception:
        return "chat/completions"

def endpoint_supports_feedback(endpoint_name):
    if not DEPENDENCIES_AVAILABLE:
        return False
        
    try:
        w = WorkspaceClient()
        endpoint = w.serving_endpoints.get(endpoint_name)
        return "feedback" in [entity.name for entity in endpoint.config.served_entities]
    except Exception:
        return False

def submit_feedback(endpoint, request_id, rating):
    """Submit feedback to the agent."""
    if not DEPENDENCIES_AVAILABLE:
        return None
        
    rating_string = "positive" if rating == 1 else "negative"
    text_assessments = [] if rating is None else [{
        "ratings": {
            "answer_correct": {"value": rating_string},
        },
        "free_text_comment": None
    }]

    proxy_payload = {
        "dataframe_records": [
            {
                "source": json.dumps({
                    "id": "databricks-iq-assistant",
                    "type": "human"
                }),
                "request_id": request_id,
                "text_assessments": json.dumps(text_assessments),
                "retrieval_assessments": json.dumps([]),
            }
        ]
    }
    try:
        w = WorkspaceClient()
        return w.api_client.do(
            method='POST',
            path=f"/serving-endpoints/{endpoint}/served-models/feedback/invocations",
            body=proxy_payload,
        )
    except Exception as e:
        logger.error(f"Failed to submit feedback: {e}")

def query_endpoint(endpoint_name, messages, return_traces):
    """Query the endpoint, returning messages and request ID."""
    if not DEPENDENCIES_AVAILABLE:
        # Fallback response when dependencies are not available
        return [{"role": "assistant", "content": "I'm sorry, but the Databricks IQ assistant is not currently available. The MLflow and Databricks SDK dependencies are required for the chatbot functionality. You can still use the analytics pages to explore your Databricks costs and usage patterns."}], None
    
    try:
        client = get_deploy_client("databricks")
        
        # Convert messages to the ResponsesAgent format
        input_items = []
        for msg in messages:
            if msg["role"] == "system":
                # System messages are typically handled differently in ResponsesAgent
                continue
            elif msg["role"] == "user":
                input_items.append({
                    "type": "message",
                    "role": "user", 
                    "content": [{"text": msg["content"]}]
                })
            elif msg["role"] == "assistant":
                input_items.append({
                    "type": "message",
                    "role": "assistant",
                    "content": [{"text": msg["content"]}]
                })
        
        # Use the ResponsesAgent format instead of OpenAI format
        inputs = {
            "input": input_items,
            "max_output_tokens": 2048,
            "temperature": 0.1,
        }
        
        if return_traces:
            inputs["databricks_options"] = {"return_trace": True}
        
        res = client.predict(endpoint=endpoint_name, inputs=inputs)
        request_id = res.get("databricks_output", {}).get("databricks_request_id")
        
        # Extract messages from the ResponsesAgent response format
        result_messages = []
        
        if "output" in res:
            # ResponsesAgent format
            for output_item in res["output"]:
                if output_item.get("type") == "message":
                    # Text message
                    content_parts = output_item.get("content", [])
                    content_text = ""
                    for part in content_parts:
                        if isinstance(part, dict) and "text" in part:
                            content_text += part["text"]
                        elif isinstance(part, str):
                            content_text += part
                    
                    if content_text:
                        result_messages.append({
                            "role": "assistant",
                            "content": content_text
                        })
                elif output_item.get("type") == "function_call":
                    # Function call
                    result_messages.append({
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [{
                            "id": output_item.get("call_id"),
                            "type": "function", 
                            "function": {
                                "name": output_item.get("name"),
                                "arguments": output_item.get("arguments", "{}")
                            }
                        }]
                    })
                elif output_item.get("type") == "function_call_output":
                    # Tool response
                    result_messages.append({
                        "role": "tool",
                        "content": output_item.get("output", ""),
                        "tool_call_id": output_item.get("call_id")
                    })
        else:
            # Fallback if response format is different
            result_messages = [{"role": "assistant", "content": str(res)}]
        
        return result_messages or [{"role": "assistant", "content": "No response found"}], request_id
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return [{"role": "assistant", "content": f"I encountered an error while processing your request: {str(e)}. Please try again or check if the endpoint is properly configured."}], None

def get_system_prompt():
    """Get the system prompt for the Databricks IQ assistant"""
    return """You are a helpful Databricks IQ Assistant that specializes in cost management and analytics for Databricks workspaces.

You can help users with:
- Analyzing Databricks costs and spending patterns
- Understanding job, cluster, and serverless compute costs
- Providing insights on model serving expenses
- Identifying cost optimization opportunities
- Explaining usage trends and patterns
- Recommending best practices for cost management
- Troubleshooting cost-related issues
- Interpreting billing and usage data

You have access to the following types of data and analytics:
- Job analytics (job costs, performance, retry patterns)
- Serverless analytics (notebook, job, and consumption costs)
- Model serving analytics (serving costs and usage)
- User analytics (individual and team spending patterns)
- Historical trends and spending alerts

Always provide clear explanations with actionable insights. When discussing costs, be specific about time periods and provide context for the numbers. Help users understand not just what they're spending, but why and how they can optimize their usage.

Focus on being helpful, accurate, and practical in your recommendations. If you need more specific information about their workspace to provide better insights, ask clarifying questions."""

def display_predefined_prompts():
    """Display predefined prompts for common cost analysis scenarios"""
    st.markdown("### 💡 Quick Analytics Questions")
    
    prompts = [
        "What are my top 5 most expensive jobs this month?",
        "Show me serverless computing cost trends over the last 30 days",
        "Which users are spending the most on compute resources?", 
        "How can I optimize my model serving costs?",
        "What are the main drivers of my Databricks spending?",
        "Analyze job retry patterns and their cost impact",
        "Compare serverless vs cluster costs for my workloads",
        "What cost optimization recommendations do you have?"
    ]
    
    cols = st.columns(2)
    for i, prompt in enumerate(prompts):
        with cols[i % 2]:
            if st.button(prompt, key=f"prompt_{i}", use_container_width=True):
                return prompt
    return None

def display_features_info():
    """Display information about available features"""
    with st.expander("📊 Available Analytics", expanded=False):
        st.markdown("""
        **Cost Analytics:**
        - Job cost analysis and trends
        - Serverless compute spending
        - Model serving cost breakdown
        - User and team spending patterns
        
        **Performance Insights:**
        - Job retry patterns and failures
        - Resource utilization analysis
        - Cost per compute hour trends
        - Spending alerts and notifications
        
        **Optimization Recommendations:**
        - Right-sizing compute resources
        - Identifying idle or underutilized assets
        - Cost allocation and chargeback insights
        - Best practices for cost management
        """)

def display_cost_examples():
    """Display common cost analysis examples and patterns"""
    with st.expander("💰 Cost Analysis Examples", expanded=False):
        st.markdown("""
        **Common Cost Questions:**
        
        ```
        Monthly Spend Analysis:
        - "What did we spend on compute last month?"
        - "Show me cost breakdown by team/user"
        - "Which jobs are driving our highest costs?"
        
        Trend Analysis:
        - "How has our spending changed over time?"
        - "What are our peak usage hours?"
        - "Identify unusual spending spikes"
        
        Optimization Opportunities:
        - "Which clusters are idle most of the time?"
        - "What jobs have high retry rates?"
        - "How can we reduce model serving costs?"
        ```
        """)

def show_chatbot():
    """Main Databricks IQ chatbot interface"""
    # Initialize session state
    if "history" not in st.session_state:
        st.session_state.history = []

    st.markdown('<div class="main-header">🤖 Databricks IQ Assistant</div>', unsafe_allow_html=True)
    
    # Display agent description
    st.markdown(f"""
    ### Your Databricks Cost Management Assistant
    
    I'm here to help you analyze costs, understand spending patterns, and optimize your Databricks usage. 
    I can provide insights on job costs, serverless compute, model serving expenses, and user spending patterns.
    
    **Endpoint:** `{SERVING_ENDPOINT}`  
    **Status:** {"🟢 Ready" if DEPENDENCIES_AVAILABLE else "🟡 Limited Mode (missing dependencies)"}
    """)
    
    # Create layout
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Render chat history
        for i, element in enumerate(st.session_state.history):
            element.render(i)
    
    with col2:
        # Predefined prompts
        selected_prompt = display_predefined_prompts()
        
        # Chat controls
        st.markdown("---")
        st.markdown("### 🛠️ Chat Controls")
        
        if st.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state.history = []
            st.rerun()
        
        # Display message count
        message_count = len(st.session_state.history)
        st.markdown(f"**Messages:** {message_count}")
        
        # Check endpoint availability
        if DEPENDENCIES_AVAILABLE:
            try:
                task_type = _get_endpoint_task_type(SERVING_ENDPOINT)
                st.success(f"✅ Assistant Ready ({task_type})")
            except Exception:
                st.error("❌ Assistant Offline")
        else:
            st.warning("⚠️ Limited Mode")
        
        # Features info and examples
        display_features_info()
        display_cost_examples()

    # Chat input box at the bottom of the page (outside columns)
    prompt = st.chat_input("Ask me about your Databricks costs and usage...")
    
    # Process selected prompt or user input
    final_prompt = selected_prompt or prompt
    
    if final_prompt:
        try:
            # Add user message to chat history
            user_msg = UserMessage(content=final_prompt)
            st.session_state.history.append(user_msg)
            user_msg.render(len(st.session_state.history) - 1)

            # Convert history to standard chat message format
            # Note: System prompt is handled within the agent itself, not sent as input
            input_messages = []
            for elem in st.session_state.history:
                for msg in elem.to_input_messages():
                    # Skip system messages as they're handled by the agent internally
                    if msg.get("role") != "system":
                        input_messages.append(msg)
            
            # Get response from the endpoint
            messages, request_id = query_endpoint(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=endpoint_supports_feedback(SERVING_ENDPOINT) if DEPENDENCIES_AVAILABLE else False
            )
            
            # Display assistant response
            with st.chat_message("assistant"):
                for message in messages:
                    render_message(message)
            
            # Add assistant response to history
            assistant_response = AssistantResponse(messages=messages, request_id=request_id)
            st.session_state.history.append(assistant_response)
            
        except Exception as e:
            st.error(f"Error processing request: {str(e)}")
            logger.error(f"Chat error: {e}")

    # Info section
    st.markdown("---")
    st.markdown("**💡 Tip:** Use the analytics pages above to explore your data, then ask me specific questions about what you discover!")