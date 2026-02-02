"""
Split Files page for generating dimension and fact table files.
"""
import streamlit as st
import pandas as pd
import os
from app.core.splitting import split_dataframe
from app.core.dq_checks import run_all_dq_checks


st.title("📂 Step 4: Split Files")

if st.session_state.df is None or st.session_state.model is None:
    st.warning("⚠️ Please generate a data model first on the Model page.")
    st.stop()

df = st.session_state.df
model = st.session_state.model

st.markdown("""
This step will split your original data file into separate dimension and fact table files
based on the generated data model. Files will be saved to the `app/output/` directory.
""")

# Split options
st.subheader("Split Options")

output_dir = st.text_input(
    "Output Directory",
    value="app/output",
    help="Directory where split files will be saved"
)

source_file_name = st.text_input(
    "Source File Name",
    value=os.path.basename(st.session_state.get('file_path', 'unknown')),
    help="Name of the source file (for metadata)"
)

# Generate split files button
if st.button("🔄 Generate Split Files", type="primary"):
    with st.spinner("Splitting data into dimension and fact tables..."):
        try:
            results = split_dataframe(
                df,
                model,
                source_file_name,
                output_dir
            )
            
            st.session_state.split_files = results['files']
            st.session_state.split_results = results
            
            if results['errors']:
                st.warning(f"⚠️ Some errors occurred: {', '.join(results['errors'])}")
            else:
                st.success("✅ Files split successfully!")
            
        except Exception as e:
            st.error(f"❌ Error splitting files: {str(e)}")
            st.exception(e)

# Display results if files are split
if st.session_state.get('split_files'):
    results = st.session_state.split_results
    
    st.subheader("📊 Split Results")
    
    # Summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Files Generated", len(results['files']))
    with col2:
        total_rows = sum(results['row_counts'].values())
        st.metric("Total Rows", f"{total_rows:,}")
    with col3:
        st.metric("Errors", len(results['errors']))
    
    # Files list
    st.subheader("📁 Generated Files")
    
    files_data = []
    for table_name, file_path in results['files'].items():
        row_count = results['row_counts'].get(table_name, 0)
        file_size = os.path.getsize(file_path) / 1024  # KB
        files_data.append({
            'Table': table_name,
            'File Path': file_path,
            'Rows': f"{row_count:,}",
            'Size (KB)': f"{file_size:.2f}"
        })
    
    files_df = pd.DataFrame(files_data)
    st.dataframe(files_df)
    
    # Download links
    st.subheader("💾 Download Files")
    
    for table_name, file_path in results['files'].items():
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                st.download_button(
                    label=f"Download {table_name}",
                    data=f.read(),
                    file_name=os.path.basename(file_path),
                    mime="text/csv",
                    key=f"download_{table_name}"
                )
    
    # Data Quality Checks
    st.subheader("✅ Data Quality Checks")
    
    if st.button("Run Data Quality Checks"):
        with st.spinner("Running data quality checks..."):
            try:
                dq_results = run_all_dq_checks(model, results['files'])
                
                if dq_results['overall_passed']:
                    st.success("✅ All data quality checks passed!")
                else:
                    st.warning("⚠️ Some data quality checks failed. See details below.")
                
                # Display check results
                checks_df = pd.DataFrame(dq_results['checks'])
                st.dataframe(checks_df)
                
                st.session_state.dq_results = dq_results
                
            except Exception as e:
                st.error(f"Error running DQ checks: {str(e)}")
                st.exception(e)
    
    # Show DQ results if available
    if st.session_state.get('dq_results'):
        dq_results = st.session_state.dq_results
        
        st.subheader("📋 DQ Check Details")
        
        for check in dq_results['checks']:
            with st.expander(f"{check['table']} - {check['check']} - {'✅' if check['passed'] else '❌'}"):
                st.write(f"**Status:** {'PASSED' if check['passed'] else 'FAILED'}")
                st.write(f"**Message:** {check.get('message', 'N/A')}")
                
                if not check['passed']:
                    if 'violations' in check:
                        st.json(check['violations'])
                    if 'duplicate_rows' in check and check['duplicate_rows']:
                        st.dataframe(pd.DataFrame(check['duplicate_rows']))
    
    st.info("👉 Proceed to **Load to Snowflake** page to upload data to Snowflake.")

else:
    st.info("👆 Click 'Generate Split Files' to create table files.")
