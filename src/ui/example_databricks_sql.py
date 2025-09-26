import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

cfg = Config()  # Ensure DATABRICKS_HOST is set in your environment

@st.cache_resource
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def read_table(table_name, conn):
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name} LIMIT 100"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

st.title("Databricks Billing Usage Viewer")

st.text(f" Here is your SQL Warehouse /sql/1.0/warehouses/{cfg.warehouse_id}")

http_path_input = st.text_input(
    "Enter your Databricks SQL Warehouse HTTP Path:",
    placeholder=f"/sql/1.0/warehouses/{cfg.warehouse_id}"
)

if http_path_input:
    conn = get_connection(http_path_input)
    df = read_table("system.billing.usage", conn)
    st.dataframe(df)
else:
    st.info("Please enter your SQL Warehouse HTTP Path to view usage data.")