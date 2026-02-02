"""
Review page for data profiling and analysis.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


st.title("🔍 Step 2: Data Review & Profiling")

if st.session_state.df is None or st.session_state.profile is None:
    st.warning("⚠️ Please upload a file first on the Upload page.")
    st.stop()

df = st.session_state.df
profile = st.session_state.profile

# Overview metrics
st.subheader("📊 Overview")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Rows", f"{profile['total_rows']:,}")
with col2:
    st.metric("Total Columns", profile['total_columns'])
with col3:
    st.metric("Data Grain", profile['grain'].replace('_', ' ').title())
with col4:
    candidate_keys_count = len(profile['candidate_keys'])
    st.metric("Candidate Keys", candidate_keys_count)

# Data preview
st.subheader("👀 Data Preview")
st.dataframe(df.head(100))

# Column profiles
st.subheader("📈 Column Profiles")

# Create profile DataFrame
profile_data = []
for col_name, col_profile in profile['column_profiles'].items():
    profile_data.append({
        'Column': col_name,
        'Type': col_profile['type'],
        'Total Rows': col_profile['total_rows'],
        'Non-Null': col_profile['non_null_count'],
        'Null Count': col_profile['null_count'],
        'Null %': col_profile['null_percentage'],
        'Distinct': col_profile['distinct_count'],
        'Distinct %': col_profile['distinct_percentage']
    })

profile_df = pd.DataFrame(profile_data)
st.dataframe(profile_df)

# Detailed column analysis
st.subheader("🔬 Detailed Column Analysis")

selected_column = st.selectbox("Select a column for detailed analysis", df.columns)

if selected_column:
    col_profile = profile['column_profiles'][selected_column]
    col_type = col_profile['type']
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Data Type", col_type)
        st.metric("Null Percentage", f"{col_profile['null_percentage']:.2f}%")
        st.metric("Distinct Values", col_profile['distinct_count'])
        st.metric("Distinct Percentage", f"{col_profile['distinct_percentage']:.2f}%")
    
    with col2:
        if col_type in ['INTEGER', 'FLOAT']:
            st.metric("Min", col_profile.get('min', 'N/A'))
            st.metric("Max", col_profile.get('max', 'N/A'))
            st.metric("Mean", f"{col_profile.get('mean', 0):.2f}" if col_profile.get('mean') else 'N/A')
            st.metric("Outliers", f"{col_profile.get('outlier_count', 0)} ({col_profile.get('outlier_percentage', 0):.2f}%)")
            
            # Distribution plot
            if col_type == 'FLOAT' or col_type == 'INTEGER':
                fig = px.histogram(df, x=selected_column, nbins=50, title=f"Distribution of {selected_column}")
                st.plotly_chart(fig, use_container_width=True)
        
        elif col_type == 'STRING':
            st.metric("Avg Length", col_profile.get('avg_length', 0))
            st.metric("Max Length", col_profile.get('max_length', 0))
            
            # Top values
            if col_profile.get('top_values'):
                top_values_df = pd.DataFrame(
                    list(col_profile['top_values'].items()),
                    columns=['Value', 'Count']
                )
                st.dataframe(top_values_df.head(10))
                
                # Bar chart
                fig = px.bar(top_values_df.head(10), x='Value', y='Count', 
                           title=f"Top 10 Values in {selected_column}")
                st.plotly_chart(fig, use_container_width=True)
        
        elif col_type == 'DATE':
            st.metric("Valid Dates", col_profile.get('valid_date_count', 0))
            st.metric("Invalid Dates", col_profile.get('invalid_date_count', 0))
            if col_profile.get('min_date'):
                st.metric("Min Date", col_profile['min_date'][:10])
                st.metric("Max Date", col_profile['max_date'][:10])

# Candidate keys
st.subheader("🔑 Candidate Keys")

if profile['candidate_keys']:
    keys_df = pd.DataFrame(profile['candidate_keys'])
    st.dataframe(keys_df)
else:
    st.info("No candidate keys found with >95% uniqueness.")

# Entity detection
st.subheader("🏗️ Entity Detection")

entities = profile['entities']

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.write("**Dimensions**")
    for dim in entities['dimensions'][:10]:
        st.write(f"- {dim}")
    if len(entities['dimensions']) > 10:
        st.write(f"... and {len(entities['dimensions']) - 10} more")

with col2:
    st.write("**Facts**")
    for fact in entities['facts'][:10]:
        st.write(f"- {fact}")
    if len(entities['facts']) > 10:
        st.write(f"... and {len(entities['facts']) - 10} more")

with col3:
    st.write("**IDs**")
    for id_col in entities['ids'][:10]:
        st.write(f"- {id_col}")
    if len(entities['ids']) > 10:
        st.write(f"... and {len(entities['ids']) - 10} more")

with col4:
    st.write("**Dates**")
    for date_col in entities['dates'][:10]:
        st.write(f"- {date_col}")
    if len(entities['dates']) > 10:
        st.write(f"... and {len(entities['dates']) - 10} more")

# Anomaly detection
st.subheader("⚠️ Anomaly Detection")

anomalies = []
for col_name, col_profile in profile['column_profiles'].items():
    if col_profile['null_percentage'] > 50:
        anomalies.append({
            'Column': col_name,
            'Issue': 'High null percentage',
            'Value': f"{col_profile['null_percentage']:.2f}%"
        })
    if col_profile.get('outlier_percentage', 0) > 10:
        anomalies.append({
            'Column': col_name,
            'Issue': 'High outlier percentage',
            'Value': f"{col_profile['outlier_percentage']:.2f}%"
        })
    if col_profile['distinct_count'] == 1:
        anomalies.append({
            'Column': col_name,
            'Issue': 'Constant value (no variation)',
            'Value': 'N/A'
        })

if anomalies:
    anomalies_df = pd.DataFrame(anomalies)
    st.dataframe(anomalies_df)
else:
    st.success("✅ No major anomalies detected!")

st.info("👉 Proceed to **Model** page to generate the data model.")
