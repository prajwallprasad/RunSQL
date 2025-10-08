import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
import time
import requests
from datetime import datetime

# ------------------------
# Database connection
# ------------------------
def get_connection():
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="Praju@2003",
            database="RunSQL"
        )
        return conn
    except Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None

# ------------------------
# Fetch SQLStoreStatus data
# ------------------------
def fetch_status():
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT * FROM SQLStoreStatus ORDER BY id DESC", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# ------------------------
# Fetch SQLStore data
# ------------------------
def fetch_sqlstore():
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT * FROM SQLStore ORDER BY id ASC", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching SQLStore data: {e}")
        return pd.DataFrame()

# ------------------------
# Trigger query execution via API with timing
# ------------------------
def trigger_query(query_id, mode="thread"):
    start_time = time.time()
    try:
        if query_id == "all":
            resp = requests.post(f"http://127.0.0.1:8000/start_all?mode={mode}")
        else:
            resp = requests.post(f"http://127.0.0.1:8000/start/{query_id}")
        end_time = time.time()
        execution_time = end_time - start_time
        result = resp.json()
        # Add execution time to result for consistency
        result['execution_time'] = execution_time
        return result
    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        return {"error": str(e), "execution_time": execution_time}

# ------------------------
# Streamlit UI
# ------------------------
st.set_page_config(page_title="SQL Execution Dashboard", layout="wide")
st.title("🔹 SQL Execution Dashboard")

# Sidebar
st.sidebar.header("Settings")
refresh_rate = st.sidebar.number_input("Refresh rate (seconds)", min_value=5, max_value=60, value=10)
st.sidebar.markdown("---")

# Mode selection for execution
mode = st.sidebar.selectbox("Select Execution Mode", ["thread", "process", "hybrid"])

# Query selection
st.sidebar.subheader("Run Queries")
query_option = st.sidebar.radio("Select option", ["Specific Query", "Run All Queries"])
if query_option == "Specific Query":
    df_sqlstore = fetch_sqlstore()
    query_ids = df_sqlstore["id"].tolist() if not df_sqlstore.empty else []
    selected_query_id = st.sidebar.selectbox("Select Query ID", query_ids)
else:
    selected_query_id = "all"

if st.sidebar.button("Run Selected Query/All"):
    result = trigger_query(selected_query_id, mode)
    st.session_state.last_result = result  # Store for display
    st.session_state.last_execution_time = result.get('execution_time', 0)  # Total time for the operation
    st.sidebar.json(result)
    st.sidebar.info(f"Execution Time: {result.get('execution_time', 0):.4f} seconds")

# Fetch data for dashboard
df_status = fetch_status()
df_sqlstore = fetch_sqlstore()

if df_status.empty:
    st.warning("No SQL execution records found. Execute some queries via FastAPI first.")
else:
    # ------------------------
    # Metrics Summary
    # ------------------------
    st.subheader("🔹 Metrics Summary")
    total_queries = len(df_status)
    success_count = len(df_status[df_status["status"] == "Success"])
    failed_count = len(df_status[df_status["status"] == "Failed"])
    stopped_count = len(df_status[df_status["status"] == "Stopped"])

    # Ensure execution_time is float
    if "execution_time" in df_status.columns:
        df_status["execution_time"] = pd.to_numeric(df_status["execution_time"], errors="coerce")
        avg_execution_time = df_status["execution_time"].mean()
    else:
        avg_execution_time = 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Executions", total_queries)
    col2.metric("Success", success_count)
    col3.metric("Failed", failed_count)
    col4.metric("Stopped", stopped_count)
    col5.metric("Avg Execution Time (s)", f"{avg_execution_time:.4f}")

    # Display last run time
    if 'last_execution_time' in st.session_state:
        st.info(f"Last Operation Execution Time: {st.session_state.last_execution_time:.4f} seconds")

    # ------------------------
    # Mode Comparison Chart
    # ------------------------
    st.subheader("🔹 Success / Failed Chart")
    chart_data = df_status.groupby("status").size().reset_index(name="count")
    st.bar_chart(chart_data.set_index("status"))

    # ------------------------
    # Live Execution Table
    # ------------------------
    st.subheader("🔹 Live Execution Table")
    selected_status = st.multiselect(
        "Filter by Status",
        options=df_status["status"].unique(),
        default=df_status["status"].unique()
    )
    filtered_df = df_status[df_status["status"].isin(selected_status)]
    st.dataframe(filtered_df)

    # ------------------------
    # Query Inspector
    # ------------------------
    st.subheader("🔹 Query Inspector")
    query_ids = df_sqlstore["id"].tolist() if not df_sqlstore.empty else []
    selected_query_id = st.selectbox("Select Query ID to inspect", query_ids)

    if selected_query_id:
        query_row = df_sqlstore[df_sqlstore["id"] == selected_query_id].iloc[0]
        st.markdown(f"**Category:** {query_row['category']}")
        st.markdown(f"**SQL Statement:** `{query_row['description']}`")
        last_result = df_status[df_status["source"] == selected_query_id].head(1)
        if not last_result.empty:
            st.markdown("**Last Execution Output:**")
            output_val = last_result["output"].values[0]
            # Safely parse string to list/dict if needed
            try:
                import ast
                if isinstance(output_val, str):
                    output_val = ast.literal_eval(output_val)
            except:
                pass
            # Display output properly
            if isinstance(output_val, (list, dict, pd.DataFrame)):
                st.dataframe(pd.DataFrame(output_val))
            else:
                st.write(output_val)

            # Show execution time
            if "execution_time" in last_result.columns:
                st.markdown(f"**Execution Time:** {last_result['execution_time'].values[0]:.2f} seconds")
            if "start_time" in last_result.columns and "end_time" in last_result.columns:
                st.markdown(f"**Started At:** {last_result['start_time'].values[0]}")
                st.markdown(f"**Ended At:** {last_result['end_time'].values[0]}")

# ------------------------
# Auto-refresh
# ------------------------
time.sleep(refresh_rate)
st.rerun()