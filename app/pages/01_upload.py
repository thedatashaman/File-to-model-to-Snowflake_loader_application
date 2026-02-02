"""
Upload page for file selection and validation.
"""
import streamlit as st
import pandas as pd
import os
from app.core.utils import load_file, validate_file, get_file_metadata
from app.core.profiling import profile_dataframe


st.title("📤 Step 1: Upload Data File")

st.markdown("""
Upload your data file to begin the modeling process. Supported formats:
- **CSV** (comma, semicolon, tab, pipe delimited)
- **JSON** / **JSONL**
- **Parquet**
- **Excel** (.xlsx, .xls)
""")

# File uploader
uploaded_file = st.file_uploader(
    "Choose a file",
    type=['csv', 'json', 'jsonl', 'parquet', 'xlsx', 'xls'],
    help="Upload a data file up to 1GB in size"
)

if uploaded_file is not None:
    # Save uploaded file temporarily
    upload_dir = "app/output/uploads"
    os.makedirs(upload_dir, exist_ok=True)
    
    file_path = os.path.join(upload_dir, uploaded_file.name)
    
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    st.session_state.uploaded_file = file_path
    
    # Detect file type
    file_ext = os.path.splitext(uploaded_file.name)[1].lower().lstrip('.')
    if file_ext == 'xls':
        file_ext = 'xlsx'  # Treat xls as xlsx
    
    # File metadata
    metadata = get_file_metadata(file_path)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("File Size", f"{metadata['size_mb']} MB")
    with col2:
        st.metric("File Type", file_ext.upper())
    with col3:
        st.metric("File Name", uploaded_file.name)
    
    # File validation options
    st.subheader("File Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        has_header = st.checkbox("First row is header", value=True)
        auto_detect = st.checkbox("Auto-detect encoding and delimiter", value=True)
    
    with col2:
        encoding = None if auto_detect else st.selectbox(
            "Encoding",
            ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252'],
            index=0
        )
        delimiter = None if auto_detect else st.selectbox(
            "Delimiter",
            [',', ';', '\t', '|'],
            index=0
        )
    
    # Validate and load button
    if st.button("🔍 Validate & Load File", type="primary"):
        with st.spinner("Validating file..."):
            is_valid, error_msg = validate_file(file_path, file_ext)
            
            if not is_valid:
                st.error(f"❌ Validation failed: {error_msg}")
            else:
                st.success("✅ File is valid!")
                
                with st.spinner("Loading file (this may take a while for large files)..."):
                    try:
                        # Load file
                        df = load_file(
                            file_path,
                            file_ext,
                            encoding=encoding,
                            delimiter=delimiter,
                            has_header=has_header
                        )
                        
                        st.session_state.df = df
                        st.session_state.file_path = file_path
                        st.session_state.file_type = file_ext
                        st.session_state.has_header = has_header
                        
                        # Quick profiling
                        with st.spinner("Profiling data..."):
                            profile = profile_dataframe(df)
                            st.session_state.profile = profile
                        
                        st.success(f"✅ File loaded successfully! {len(df):,} rows, {len(df.columns)} columns")
                        
                        # Show preview
                        st.subheader("Data Preview")
                        st.dataframe(df.head(100))
                        
                        # Show basic stats
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Rows", f"{len(df):,}")
                        with col2:
                            st.metric("Total Columns", len(df.columns))
                        with col3:
                            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
                        
                        st.info("👉 Proceed to **Review** page to see detailed profiling results.")
                        
                    except Exception as e:
                        st.error(f"❌ Error loading file: {str(e)}")
                        st.exception(e)

elif st.session_state.uploaded_file:
    st.info("📁 File already uploaded. Use the sidebar to navigate to other pages.")
