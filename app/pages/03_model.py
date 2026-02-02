"""
Model page for data model generation and review.
"""
import streamlit as st
import pandas as pd
from app.core.modeling import infer_data_model, generate_snowflake_ddl, generate_mermaid_erd
import os
from pathlib import Path


st.title("🏗️ Step 3: Data Model Generation")

if st.session_state.df is None or st.session_state.profile is None:
    st.warning("⚠️ Please upload and review a file first.")
    st.stop()

df = st.session_state.df
profile = st.session_state.profile

st.markdown("""
The app will automatically generate a data model based on best practices:
- **Star Schema** for analytics-oriented data
- **3NF** for transactional data
- Automatic dimension and fact table identification
- Surrogate keys and relationships
""")

# Model generation options
st.subheader("Model Options")

col1, col2 = st.columns(2)

with col1:
    model_type = st.radio(
        "Model Type",
        ["Auto-detect", "Star Schema", "3NF"],
        index=0,
        help="Auto-detect will choose the best model type based on data characteristics"
    )

with col2:
    include_date_dim = st.checkbox(
        "Include Date Dimension",
        value=len(profile['entities']['dates']) > 0,
        help="Automatically create a date dimension table if date columns exist"
    )

# Generate model button
if st.button("🚀 Generate Data Model", type="primary"):
    with st.spinner("Generating data model..."):
        try:
            # Infer model
            model = infer_data_model(df, profile)
            
            # Override date dimension if needed
            if not include_date_dim and 'DIM_DATE' in model.tables:
                del model.tables['DIM_DATE']
            
            st.session_state.model = model
            
            st.success("✅ Data model generated successfully!")
            
        except Exception as e:
            st.error(f"❌ Error generating model: {str(e)}")
            st.exception(e)

# Display model if generated
if st.session_state.model:
    model = st.session_state.model
    
    st.subheader("📋 Generated Model Summary")
    
    # Model statistics
    fact_tables = [t for t in model.tables.values() if t['type'] == 'FACT']
    dim_tables = [t for t in model.tables.values() if t['type'] == 'DIM']
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Tables", len(model.tables))
    with col2:
        st.metric("Fact Tables", len(fact_tables))
    with col3:
        st.metric("Dimension Tables", len(dim_tables))
    
    # Tables overview
    st.subheader("📊 Tables")
    
    for table_name, table_def in model.tables.items():
        with st.expander(f"{table_name} ({table_def['type']})"):
            st.write(f"**Type:** {table_def['type']}")
            st.write(f"**Primary Key:** {', '.join(table_def['primary_key'])}")
            if table_def.get('grain'):
                st.write(f"**Grain:** {table_def['grain']}")
            
            st.write("**Columns:**")
            columns_df = pd.DataFrame([
                {
                    'Name': col['name'],
                    'Type': col['type'],
                    'Nullable': col.get('nullable', True),
                    'PK': col.get('is_pk', False),
                    'FK': col.get('is_fk', False)
                }
                for col in table_def['columns']
            ])
            st.dataframe(columns_df)
    
    # Relationships
    if model.relationships:
        st.subheader("🔗 Relationships")
        relationships_df = pd.DataFrame(model.relationships)
        st.dataframe(relationships_df)
    
    # ERD Diagram
    st.subheader("📐 Entity Relationship Diagram")
    
    try:
        mermaid_erd = generate_mermaid_erd(model)
        st.code(mermaid_erd, language='mermaid')
        
        # Note: Streamlit doesn't natively render Mermaid, but users can copy to Mermaid editor
        st.info("💡 Copy the code above to https://mermaid.live/ to visualize the ERD")
    except Exception as e:
        st.warning(f"Could not generate ERD: {str(e)}")
    
    # Snowflake DDL
    st.subheader("❄️ Snowflake DDL")
    
    database = st.text_input("Database Name", value="MY_DATABASE")
    schema = st.text_input("Schema Name", value="MY_SCHEMA")
    
    if st.button("Generate DDL"):
        try:
            ddl = generate_snowflake_ddl(model, database, schema)
            st.code(ddl, language='sql')
            
            # Save DDL
            output_dir = "app/output/model"
            os.makedirs(output_dir, exist_ok=True)
            ddl_path = os.path.join(output_dir, "snowflake_ddl.sql")
            with open(ddl_path, 'w') as f:
                f.write(ddl)
            st.success(f"✅ DDL saved to {ddl_path}")
            
        except Exception as e:
            st.error(f"Error generating DDL: {str(e)}")
    
    # Model summary document
    st.subheader("📄 Model Documentation")
    
    if st.button("Generate Model Summary"):
        try:
            summary = f"""# Data Model Summary

## Generated: {pd.Timestamp.now()}

## Overview
- Total Tables: {len(model.tables)}
- Fact Tables: {len(fact_tables)}
- Dimension Tables: {len(dim_tables)}
- Relationships: {len(model.relationships)}

## Tables

"""
            for table_name, table_def in model.tables.items():
                summary += f"### {table_name} ({table_def['type']})\n"
                summary += f"- Primary Key: {', '.join(table_def['primary_key'])}\n"
                summary += f"- Columns: {len(table_def['columns'])}\n\n"
            
            summary += "\n## Relationships\n\n"
            for rel in model.relationships:
                summary += f"- {rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']}\n"
            
            # Save summary
            output_dir = "app/output/model"
            os.makedirs(output_dir, exist_ok=True)
            summary_path = os.path.join(output_dir, "model_summary.md")
            with open(summary_path, 'w') as f:
                f.write(summary)
            
            st.success(f"✅ Model summary saved to {summary_path}")
            st.code(summary, language='markdown')
            
        except Exception as e:
            st.error(f"Error generating summary: {str(e)}")
    
    st.info("👉 Proceed to **Split Files** page to generate table files.")

else:
    st.info("👆 Click 'Generate Data Model' to create the model.")
