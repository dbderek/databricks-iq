"""
Databricks IQ Chatbot Interface
Chatbot functionality for the Databricks IQ application
"""

import streamlit as st
from pathlib import Path
import sys

# Add the parent directory to the path so we can import the agent
sys.path.append(str(Path(__file__).parent.parent))

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