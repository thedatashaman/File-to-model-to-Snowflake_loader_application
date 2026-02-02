"""
Logs page for viewing ingestion history and errors.
"""
import streamlit as st
import pandas as pd
from app.core.snowflake_loader import SnowflakeLoader


st.title("📋 Step 6: Ingestion Logs")

config = st.session_state.get('snowflake_config') or {}

# Check if we have connection details (loaded from .env)
if not all([config.get('SNOWFLAKE_ACCOUNT'), config.get('SNOWFLAKE_USER'),
           (config.get('SNOWFLAKE_PASSWORD') or config.get('SNOWFLAKE_PRIVATE_KEY'))]):
    st.warning("⚠️ Set **SNOWFLAKE_ACCOUNT**, **SNOWFLAKE_USER**, and **SNOWFLAKE_PASSWORD** (or **SNOWFLAKE_PRIVATE_KEY**) in your `.env` file to view logs.")
    st.stop()

# Connect to Snowflake
st.subheader("🔌 Connect to View Logs")

if st.button("Connect & Load Logs"):
    with st.spinner("Connecting to Snowflake and loading logs..."):
        try:
            loader = SnowflakeLoader(config)
            
            if not loader.connect():
                st.error("❌ Failed to connect to Snowflake.")
                st.stop()
            
            # Query ingestion runs
            try:
                loader.cursor.execute("""
                    SELECT 
                        RUN_ID,
                        RUN_START_TS,
                        RUN_END_TS,
                        STATUS,
                        SOURCE_FILE_NAME,
                        TOTAL_TABLES,
                        TABLES_LOADED,
                        TABLES_FAILED,
                        ERROR_MESSAGE
                    FROM INGESTION_RUNS
                    ORDER BY RUN_START_TS DESC
                    LIMIT 100
                """)
                
                runs_data = loader.cursor.fetchall()
                columns = [desc[0] for desc in loader.cursor.description]
                runs_df = pd.DataFrame(runs_data, columns=columns)
                
                st.session_state.runs_df = runs_df
                
            except Exception as e:
                st.warning(f"Could not load ingestion runs (table may not exist): {str(e)}")
                runs_df = pd.DataFrame()
            
            # Query table status
            try:
                loader.cursor.execute("""
                    SELECT 
                        STATUS_ID,
                        RUN_ID,
                        TABLE_NAME,
                        STATUS,
                        ROWS_LOADED,
                        ROWS_EXPECTED,
                        LOAD_START_TS,
                        LOAD_END_TS,
                        ERROR_MESSAGE
                    FROM INGESTION_TABLE_STATUS
                    ORDER BY LOAD_START_TS DESC
                    LIMIT 500
                """)
                
                status_data = loader.cursor.fetchall()
                status_columns = [desc[0] for desc in loader.cursor.description]
                status_df = pd.DataFrame(status_data, columns=status_columns)
                
                st.session_state.status_df = status_df
                
            except Exception as e:
                st.warning(f"Could not load table status (table may not exist): {str(e)}")
                status_df = pd.DataFrame()
            
            # Query errors
            try:
                loader.cursor.execute("""
                    SELECT 
                        ERROR_ID,
                        RUN_ID,
                        TABLE_NAME,
                        ROW_NUMBER,
                        ERROR_MESSAGE,
                        REJECTED_ROW
                    FROM INGESTION_ERRORS
                    ORDER BY ERROR_ID DESC
                    LIMIT 500
                """)
                
                errors_data = loader.cursor.fetchall()
                error_columns = [desc[0] for desc in loader.cursor.description]
                errors_df = pd.DataFrame(errors_data, columns=error_columns)
                
                st.session_state.errors_df = errors_df
                
            except Exception as e:
                st.warning(f"Could not load errors (table may not exist): {str(e)}")
                errors_df = pd.DataFrame()
            
            loader.disconnect()
            
            st.success("✅ Logs loaded successfully!")
            
        except Exception as e:
            st.error(f"❌ Error loading logs: {str(e)}")
            st.exception(e)

# Display ingestion runs
if st.session_state.get('runs_df') is not None:
    runs_df = st.session_state.runs_df
    
    if not runs_df.empty:
        st.subheader("📊 Ingestion Runs")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Runs", len(runs_df))
        with col2:
            successful = len(runs_df[runs_df['STATUS'] == 'SUCCESS'])
            st.metric("Successful", successful)
        with col3:
            failed = len(runs_df[runs_df['STATUS'] == 'FAILED'])
            st.metric("Failed", failed)
        with col4:
            partial = len(runs_df[runs_df['STATUS'] == 'PARTIAL'])
            st.metric("Partial", partial)
        
        # Runs table
        st.dataframe(runs_df)
        
        # Filter by run
        if len(runs_df) > 0:
            selected_run_id = st.selectbox(
                "Select a run to view details",
                runs_df['RUN_ID'].tolist()
            )
            
            if selected_run_id:
                # Show table status for selected run
                if st.session_state.get('status_df') is not None:
                    status_df = st.session_state.status_df
                    run_status = status_df[status_df['RUN_ID'] == selected_run_id]
                    
                    if not run_status.empty:
                        st.subheader(f"📋 Table Status for Run {selected_run_id}")
                        st.dataframe(run_status)
                
                # Show errors for selected run
                if st.session_state.get('errors_df') is not None:
                    errors_df = st.session_state.errors_df
                    run_errors = errors_df[errors_df['RUN_ID'] == selected_run_id]
                    
                    if not run_errors.empty:
                        st.subheader(f"⚠️ Errors for Run {selected_run_id}")
                        st.dataframe(run_errors)
    else:
        st.info("No ingestion runs found. Run a load operation first.")

# Display all table status
if st.session_state.get('status_df') is not None:
    status_df = st.session_state.status_df
    
    if not status_df.empty:
        st.subheader("📊 All Table Status")
        st.dataframe(status_df)

# Display all errors
if st.session_state.get('errors_df') is not None:
    errors_df = st.session_state.errors_df
    
    if not errors_df.empty:
        st.subheader("⚠️ All Errors")
        st.dataframe(errors_df)
