"dbx-lakespend-endpoint""""
SeaTac Data Quality Monitoring System
Team BuzzForce - Hackathon 2025-09

Resource Tagging Agent Chatbot Module
"""

import logging
import os
import streamlit as st
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List, Dict, Any
import mlflow
from mlflow.deployments import get_deploy_client
from databricks.sdk import WorkspaceClient
import json
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
SERVING_ENDPOINT = os.getenv('SERVING_ENDPOINT', 'dbx-lakespend-endpoint')

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
                st.markdown(f"🏷️ Calling **`{fn_name}`** with:\n```json\n{args}\n```")
    elif msg["role"] == "tool":
        st.markdown("🛠️ Tool Response:")
        st.code(msg["content"], language="json")

@st.fragment
def render_assistant_message_feedback(i, request_id):
    """Render feedback UI for assistant messages."""
    def save_feedback(index):
        if SERVING_ENDPOINT:
            submit_feedback(
                endpoint=SERVING_ENDPOINT,
                request_id=request_id,
                rating=st.session_state[f"feedback_{index}"]
            )
    
    st.feedback("thumbs", key=f"feedback_{i}", on_change=save_feedback, args=[i])

# Model serving utilities (adapted from example)
def _get_endpoint_task_type(endpoint_name: str) -> str:
    """Get the task type of a serving endpoint."""
    try:
        w = WorkspaceClient()
        ep = w.serving_endpoints.get(endpoint_name)
        return ep.task if ep.task else "chat/completions"
    except Exception:
        return "chat/completions"

def endpoint_supports_feedback(endpoint_name):
    try:
        w = WorkspaceClient()
        endpoint = w.serving_endpoints.get(endpoint_name)
        return "feedback" in [entity.name for entity in endpoint.config.served_entities]
    except Exception:
        return False

def submit_feedback(endpoint, request_id, rating):
    """Submit feedback to the agent."""
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
                    "id": "resource-tagging-agent",
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

def _convert_to_responses_format(messages):
    """Convert chat messages to ResponsesAgent API format."""
    input_messages = []
    for msg in messages:
        if msg["role"] == "user":
            input_messages.append({
                "type": "message",
                "id": str(uuid.uuid4()),
                "content": [{"type": "input_text", "text": msg["content"]}],
                "role": "user"
            })
        elif msg["role"] == "assistant":
            # Handle assistant messages with tool calls
            if msg.get("tool_calls"):
                # Add function calls
                for tool_call in msg["tool_calls"]:
                    input_messages.append({
                        "type": "function_call",
                        "id": tool_call["id"],
                        "call_id": tool_call["id"],
                        "name": tool_call["function"]["name"],
                        "arguments": tool_call["function"]["arguments"]
                    })
                # Add assistant message if it has content
                if msg.get("content"):
                    input_messages.append({
                        "type": "message",
                        "id": str(uuid.uuid4()),
                        "content": [{"type": "output_text", "text": msg["content"]}],
                        "role": "assistant"
                    })
            else:
                # Regular assistant message
                input_messages.append({
                    "type": "message",
                    "id": str(uuid.uuid4()),
                    "content": [{"type": "output_text", "text": msg["content"]}],
                    "role": "assistant"
                })
        elif msg["role"] == "tool":
            input_messages.append({
                "type": "function_call_output",
                "call_id": msg.get("tool_call_id"),
                "output": msg["content"]
            })
        elif msg["role"] == "system":
            # System messages can be converted to user messages with special formatting
            input_messages.append({
                "type": "message",
                "id": str(uuid.uuid4()),
                "content": [{"type": "input_text", "text": f"System: {msg['content']}"}],
                "role": "user"
            })
    return input_messages

def query_endpoint_stream(endpoint_name: str, messages: list[dict[str, str]], return_traces: bool):
    """Stream responses from the ResponsesAgent endpoint."""
    try:
        client = get_deploy_client("databricks")
        
        # Convert messages to ResponsesAgent format
        input_messages = _convert_to_responses_format(messages)
        
        inputs = {
            "input": input_messages,
            "context": {},
            "stream": True
        }
        if return_traces:
            inputs["databricks_options"] = {"return_trace": True}

        for chunk in client.predict_stream(endpoint=endpoint_name, inputs=inputs):
            yield chunk
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise e

def query_endpoint(endpoint_name, messages, return_traces):
    """Query the ResponsesAgent endpoint, returning messages and request ID."""
    try:
        client = get_deploy_client("databricks")
        
        # Convert messages to ResponsesAgent format
        input_messages = _convert_to_responses_format(messages)
        
        inputs = {
            "input": input_messages,
            "context": {}
        }
        if return_traces:
            inputs["databricks_options"] = {"return_trace": True}
        
        res = client.predict(endpoint=endpoint_name, inputs=inputs)
        request_id = res.get("databricks_output", {}).get("databricks_request_id")
        
        # Extract messages from the response
        result_messages = []
        output_items = res.get("output", [])
        
        for item in output_items:
            item_type = item.get("type")
            
            if item_type == "message":
                # Extract text content from message
                text_content = ""
                content_parts = item.get("content", [])
                
                for content_part in content_parts:
                    if content_part.get("type") == "output_text":
                        text_content += content_part.get("text", "")
                
                if text_content:
                    result_messages.append({
                        "role": "assistant",
                        "content": text_content
                    })
                    
            elif item_type == "function_call":
                # Handle function calls
                call_id = item.get("call_id")
                function_name = item.get("name")
                arguments = item.get("arguments", "")
                
                tool_calls = [{
                    "id": call_id,
                    "type": "function", 
                    "function": {
                        "name": function_name,
                        "arguments": arguments
                    }
                }]
                result_messages.append({
                    "role": "assistant",
                    "content": "",
                    "tool_calls": tool_calls
                })
                
            elif item_type == "function_call_output":
                # Handle function call output/result
                call_id = item.get("call_id")
                output_content = item.get("output", "")
                
                result_messages.append({
                    "role": "tool",
                    "content": output_content,
                    "tool_call_id": call_id
                })
        
        return result_messages or [{"role": "assistant", "content": "No response found"}], request_id
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return [{"role": "assistant", "content": f"Error: {str(e)}"}], None

def query_responses_endpoint_and_render(input_messages):
    """Handle ResponsesAgent streaming format using MLflow types."""
    from mlflow.types.responses import ResponsesAgentStreamEvent
    
    with st.chat_message("assistant"):
        response_area = st.empty()
        response_area.markdown("_Processing tagging request..._")
        
        # Track all the messages that need to be rendered in order
        all_messages = []
        request_id = None

        try:
            for raw_event in query_endpoint_stream(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=endpoint_supports_feedback(SERVING_ENDPOINT)
            ):
                # Extract databricks_output for request_id
                if "databricks_output" in raw_event:
                    req_id = raw_event["databricks_output"].get("databricks_request_id")
                    if req_id:
                        request_id = req_id
                
                # Parse using MLflow streaming event types
                if "type" in raw_event:
                    event = ResponsesAgentStreamEvent.model_validate(raw_event)
                    
                    if hasattr(event, 'item') and event.item:
                        item = event.item  # This is a dict, not a parsed object
                        
                        if item.get("type") == "message":
                            # Extract text content from message if present
                            content_parts = item.get("content", [])
                            for content_part in content_parts:
                                if content_part.get("type") == "output_text":
                                    text = content_part.get("text", "")
                                    if text:
                                        all_messages.append({
                                            "role": "assistant",
                                            "content": text
                                        })
                            
                        elif item.get("type") == "function_call":
                            # Tool call
                            call_id = item.get("call_id")
                            function_name = item.get("name")
                            arguments = item.get("arguments", "")
                            
                            # Add to messages for history
                            all_messages.append({
                                "role": "assistant",
                                "content": "",
                                "tool_calls": [{
                                    "id": call_id,
                                    "type": "function",
                                    "function": {
                                        "name": function_name,
                                        "arguments": arguments
                                    }
                                }]
                            })
                            
                        elif item.get("type") == "function_call_output":
                            # Tool call output/result
                            call_id = item.get("call_id")
                            output = item.get("output", "")
                            
                            # Add to messages for history
                            all_messages.append({
                                "role": "tool",
                                "content": output,
                                "tool_call_id": call_id
                            })
                
                # Update the display by rendering all accumulated messages
                if all_messages:
                    response_area.empty()
                    with response_area.container():
                        for msg in all_messages:
                            render_message(msg)

            return AssistantResponse(messages=all_messages, request_id=request_id)
        except Exception:
            response_area.markdown("_Ran into an error. Retrying without streaming..._")
            messages, request_id = query_endpoint(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=endpoint_supports_feedback(SERVING_ENDPOINT)
            )
            response_area.empty()
            with response_area.container():
                for message in messages:
                    render_message(message)
            return AssistantResponse(messages=messages, request_id=request_id)

def get_system_prompt():
    """Get the system prompt for the resource tagging agent"""
    return """You are a helpful Databricks Resource Tagging Agent assistant that specializes in tag management for Databricks resources.

You can help users with:
- Managing tags on clusters, SQL warehouses, jobs, and Delta Live Tables pipelines  
- Finding resources by their tags
- Generating compliance reports for required tags
- Bulk updating tags across multiple resources
- Understanding tag naming conventions and best practices
- Troubleshooting tag-related issues

Available MCP Tools for Databricks Resource Tagging:

**Cluster Tag Management:**
- list_cluster_tags: List all tags for a specific cluster
- update_cluster_tags: Update tags on a cluster (add, modify, or remove)
- get_all_clusters_with_tags: Get all clusters and their current tags

**SQL Warehouse Tag Management:**
- list_warehouse_tags: List all tags for a specific SQL warehouse
- update_warehouse_tags: Update tags on a SQL warehouse
- get_all_warehouses_with_tags: Get all SQL warehouses and their current tags

**Job Tag Management:**
- list_job_tags: List all tags for a specific job
- update_job_tags: Update tags on a job
- get_all_jobs_with_tags: Get all jobs and their current tags

**Pipeline Tag Management:**
- list_pipeline_tags: List all tags for a specific pipeline
- update_pipeline_tags: Update tags on a pipeline
- get_all_pipelines_with_tags: Get all pipelines and their current tags

**Bulk Operations:**
- bulk_update_tags: Update tags across multiple resources at once
- find_resources_by_tag: Find all resources that have specific tag keys or values
- tag_compliance_report: Generate a report on tag compliance across resources

Always provide clear explanations of what actions you're taking and what the results mean. Help users follow Databricks tagging best practices."""

def display_predefined_prompts():
    """Display predefined prompts for common tagging scenarios"""
    st.markdown("### 🏷️ Quick Tagging Actions")
    
    prompts = [
        "Show me all clusters and their current tags",
        "How do I add cost center tags to multiple resources?", 
        "Generate a compliance report for required tags",
        "Find all resources tagged with environment=production",
        "What are the best practices for Databricks resource tagging?",
        "Help me bulk update tags for my development resources",
        "Show me all untagged clusters",
        "How do I remove obsolete tags from pipelines?"
    ]
    
    cols = st.columns(2)
    for i, prompt in enumerate(prompts):
        with cols[i % 2]:
            if st.button(prompt, key=f"prompt_{i}", use_container_width=True):
                return prompt
    return None

def display_mcp_tools_info():
    """Display information about available MCP tools"""
    with st.expander("🛠️ Available Tagging Tools", expanded=False):
        st.markdown("""
        **Cluster Management:**
        - List/update cluster tags
        - Get all clusters with tags
        
        **SQL Warehouse Management:**
        - List/update warehouse tags
        - Get all warehouses with tags
        
        **Job Management:**
        - List/update job tags
        - Get all jobs with tags
        
        **Pipeline Management:**
        - List/update pipeline tags
        - Get all pipelines with tags
        
        **Bulk Operations:**
        - Bulk tag updates across resources
        - Find resources by tag criteria
        - Generate compliance reports
        """)

def display_tagging_examples():
    """Display common tagging examples and patterns"""
    with st.expander("📚 Tagging Examples", expanded=False):
        st.markdown("""
        **Common Tag Patterns:**
        
        ```
        Environment Tags:
        - environment: production | staging | development
        - env: prod | stage | dev
        
        Ownership Tags:
        - team: data-engineering | analytics | ml-ops
        - owner: john.doe@company.com
        - project: customer-analytics
        
        Cost Management Tags:
        - cost-center: eng-001 | sales-002
        - budget-code: fy25-q3-ml
        - billing-group: data-platform
        
        Compliance Tags:
        - data-classification: public | internal | confidential
        - retention-policy: 7-years | 3-years
        - compliance: sox | gdpr | hipaa
        ```
        """)

def show_chatbot():
    """Main Databricks LakeSpend chatbot interface"""
    # Initialize session state
    if "history" not in st.session_state:
        st.session_state.history = []

    st.markdown('<div class="main-header">🏷️ Resource Tagging Agent</div>', unsafe_allow_html=True)
    
    # Display agent description
    st.markdown(f"""
    ### Your Databricks Resource Tagging Assistant
    
    I'm here to help you manage tags across your Databricks resources including clusters, SQL warehouses, jobs, and pipelines. 
    I can help with individual resource tagging, bulk operations, compliance reporting, and tagging best practices.
    
    **Endpoint:** `{SERVING_ENDPOINT}`  
    **Connected to MCP Server:** Ready to perform tag management operations!
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
        try:
            task_type = _get_endpoint_task_type(SERVING_ENDPOINT)
            st.success(f"✅ Agent Ready ({task_type})")
        except Exception:
            st.error("❌ Agent Offline")
        
        # MCP Tools info and examples
        display_mcp_tools_info()
        display_tagging_examples()

    # Chat input box at the bottom of the page (outside columns)
    prompt = st.chat_input("Ask me about resource tagging...")
    
    # Process selected prompt or user input
    final_prompt = selected_prompt or prompt
    
    if final_prompt:
        try:
            # Get the task type for this endpoint
            task_type = _get_endpoint_task_type(SERVING_ENDPOINT)
            
            # Add system message if this is the first message
            if not st.session_state.history:
                system_msg = get_system_prompt()
                st.session_state.history.append(AssistantResponse(
                    messages=[{"role": "system", "content": system_msg}],
                    request_id=None
                ))
            
            # Add user message to chat history
            user_msg = UserMessage(content=final_prompt)
            st.session_state.history.append(user_msg)
            user_msg.render(len(st.session_state.history) - 1)

            # Convert history to standard chat message format
            input_messages = [msg for elem in st.session_state.history for msg in elem.to_input_messages()]
            
            # Handle the response using ResponsesAgent format
            assistant_response = query_responses_endpoint_and_render(input_messages)
            
            # Add assistant response to history
            st.session_state.history.append(assistant_response)
            
        except Exception as e:
            st.error(f"Error processing request: {str(e)}")
            logger.error(f"Chat error: {e}")

    # Info section
    st.markdown("---")
