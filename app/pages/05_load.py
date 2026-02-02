"""
Load to Snowflake page for loading data using connection from .env.
"""
import streamlit as st
import os
from app.core.snowflake_loader import SnowflakeLoader


st.title("❄️ Step 5: Load to Snowflake")

if st.session_state.get('split_files') is None:
    st.warning("⚠️ Please split files first on the Split Files page.")
    st.stop()

if st.session_state.model is None:
    st.warning("⚠️ Please generate a data model first.")
    st.stop()

# Use Snowflake config from .env (set in streamlit_app.py)
config = st.session_state.snowflake_config
account = config.get('SNOWFLAKE_ACCOUNT', '')
user = config.get('SNOWFLAKE_USER', '')
password = config.get('SNOWFLAKE_PASSWORD', '')
private_key = config.get('SNOWFLAKE_PRIVATE_KEY', '')
warehouse = config.get('SNOWFLAKE_WAREHOUSE', '')
database = config.get('SNOWFLAKE_DATABASE', '')
schema = config.get('SNOWFLAKE_SCHEMA', '')

# Require connection details from .env
if not account or not user or not (password or private_key):
    st.error("❌ Snowflake connection is not configured. Set **SNOWFLAKE_ACCOUNT**, **SNOWFLAKE_USER**, and either **SNOWFLAKE_PASSWORD** or **SNOWFLAKE_PRIVATE_KEY** in your `.env` file.")
    st.stop()

if not warehouse or not database or not schema:
    st.error("❌ Set **SNOWFLAKE_WAREHOUSE**, **SNOWFLAKE_DATABASE**, and **SNOWFLAKE_SCHEMA** in your `.env` file.")
    st.stop()

st.subheader("🔧 Connection")
st.info(f"Using Snowflake connection from **.env** → database **{database}**, schema **{schema}**.")

# Test connection
st.subheader("🔌 Connection Test")

col1, col2 = st.columns(2)

col1, col2 = st.columns(2)
with col1:
    if st.button("Test Connection"):
        with st.spinner("Testing connection..."):
            loader = SnowflakeLoader(config)
            if loader.connect():
                st.success("✅ Connection successful!")
                loader.disconnect()
            else:
                st.error("❌ Connection failed. Check your `.env` credentials.")

with col2:
    if st.button("Check Database/Schema"):
        with st.spinner("Checking database and schema..."):
            try:
                loader = SnowflakeLoader(config)
                if loader.connect():
                    db_exists = loader.database_exists(database)
                    if db_exists:
                        st.info(f"✅ Database '{database}' exists")
                        schema_exists = loader.schema_exists(database, schema)
                        if schema_exists:
                            st.info(f"✅ Schema '{schema}' exists in database '{database}'")
                        else:
                            st.warning(f"⚠️ Schema '{schema}' does not exist (will be created)")
                    else:
                        st.warning(f"⚠️ Database '{database}' does not exist (will be created)")
                    loader.disconnect()
                else:
                    st.error("❌ Failed to connect. Check your `.env` credentials.")
            except Exception as e:
                st.error(f"❌ Error checking database/schema: {str(e)}")

# Load data
st.subheader("📤 Load Data to Snowflake")

st.markdown("""
This will:
1. Check if database and schema exist, create them if they don't (or reuse if they do)
2. Create internal stage for file uploads
3. Create tables based on the data model
4. Upload files to stage
5. Load data using COPY INTO
6. Validate row counts

**Note:** If the database or schema already exists, they will be reused. Only tables will be created/replaced.
""")

source_file_name = st.text_input(
    "Source File Name (for logging)",
    value=os.path.basename(st.session_state.get('file_path', 'unknown'))
)

if st.button("🚀 Load to Snowflake", type="primary"):
    with st.spinner("Loading data to Snowflake (this may take a while)..."):
        try:
            loader = SnowflakeLoader(config)

            if not loader.connect():
                st.error("❌ Failed to connect to Snowflake. Check your `.env` credentials.")
                st.stop()

            load_results = loader.load_all_tables(
                st.session_state.model,
                st.session_state.split_files,
                source_file_name
            )

            loader.disconnect()

            if 'database_info' in load_results:
                db_info = load_results['database_info']
                schema_info = load_results['schema_info']
                info_col1, info_col2 = st.columns(2)
                with info_col1:
                    if db_info.get('existed'):
                        st.info(f"📁 Database '{database}' already existed (reused)")
                    else:
                        st.success(f"✨ Database '{database}' was created")
                with info_col2:
                    if schema_info.get('existed'):
                        st.info(f"📁 Schema '{schema}' already existed (reused)")
                    else:
                        st.success(f"✨ Schema '{schema}' was created")

            if load_results['overall_success']:
                st.success("✅ All tables loaded successfully!")
            else:
                st.warning("⚠️ Some tables failed to load. See details below.")

            st.subheader("📊 Load Results")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Run ID", load_results.get('run_id', 'N/A'))
            with col2:
                total_tables = len(load_results['tables'])
                successful = sum(1 for t in load_results['tables'].values() if t['success'])
                st.metric("Tables Loaded", f"{successful}/{total_tables}")
            with col3:
                total_rows = sum(t.get('rows_loaded', 0) for t in load_results['tables'].values())
                st.metric("Total Rows Loaded", f"{total_rows:,}")

            st.subheader("📋 Table Load Details")
            import pandas as pd
            table_results_data = [
                {
                    'Table': table_name,
                    'Status': '✅ Success' if table_result['success'] else '❌ Failed',
                    'Rows Loaded': f"{table_result.get('rows_loaded', 0):,}",
                    'Error': table_result.get('error', 'N/A')
                }
                for table_name, table_result in load_results['tables'].items()
            ]
            table_results_df = pd.DataFrame(table_results_data)
            st.dataframe(table_results_df)

            if load_results['errors']:
                st.subheader("⚠️ Errors")
                for error in load_results['errors']:
                    st.error(error)

            st.session_state.load_results = load_results
            st.info("👉 Check the **Logs** page to view detailed ingestion logs.")

        except Exception as e:
            st.error(f"❌ Error loading data: {str(e)}")
            st.exception(e)

# Show previous load results
if st.session_state.get('load_results'):
    st.subheader("📜 Previous Load Results")
    
    prev_results = st.session_state.load_results
    st.json(prev_results)
