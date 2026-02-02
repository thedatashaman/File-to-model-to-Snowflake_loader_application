"""
Main Streamlit application for File-to-Model-to-Snowflake Loader.
"""
import streamlit as st
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Build Snowflake config from .env
def _snowflake_config_from_env():
    return {
        'SNOWFLAKE_ACCOUNT': os.environ.get('SNOWFLAKE_ACCOUNT', ''),
        'SNOWFLAKE_USER': os.environ.get('SNOWFLAKE_USER', ''),
        'SNOWFLAKE_PASSWORD': os.environ.get('SNOWFLAKE_PASSWORD', ''),
        'SNOWFLAKE_PRIVATE_KEY': os.environ.get('SNOWFLAKE_PRIVATE_KEY', ''),
        'SNOWFLAKE_WAREHOUSE': os.environ.get('SNOWFLAKE_WAREHOUSE', ''),
        'SNOWFLAKE_DATABASE': os.environ.get('SNOWFLAKE_DATABASE', ''),
        'SNOWFLAKE_SCHEMA': os.environ.get('SNOWFLAKE_SCHEMA', ''),
        'SNOWFLAKE_ROLE': os.environ.get('SNOWFLAKE_ROLE', ''),
    }

# Set page config
st.set_page_config(
    page_title="Data Modeler & Snowflake Loader",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'uploaded_file' not in st.session_state:
    st.session_state.uploaded_file = None
if 'df' not in st.session_state:
    st.session_state.df = None
if 'profile' not in st.session_state:
    st.session_state.profile = None
if 'model' not in st.session_state:
    st.session_state.model = None
if 'split_files' not in st.session_state:
    st.session_state.split_files = None
if 'snowflake_config' not in st.session_state:
    st.session_state.snowflake_config = _snowflake_config_from_env()

# Main navigation
st.sidebar.title("📊 Data Modeler")
st.sidebar.markdown("---")

pages = {
    "1️⃣ Upload": "app/pages/01_upload.py",
    "2️⃣ Review": "app/pages/02_review.py",
    "3️⃣ Model": "app/pages/03_model.py",
    "4️⃣ Split Files": "app/pages/04_split.py",
    "5️⃣ Load to Snowflake": "app/pages/05_load.py",
    "6️⃣ Logs": "app/pages/06_logs.py"
}

selected_page = st.sidebar.radio(
    "Navigation",
    list(pages.keys()),
    index=0
)

# Load selected page
page_path = pages[selected_page]
if os.path.exists(page_path):
    with open(page_path, 'r', encoding='utf-8') as f:
        exec(f.read())
else:
    st.error(f"Page file not found: {page_path}")
