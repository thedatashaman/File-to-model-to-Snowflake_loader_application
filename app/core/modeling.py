"""
Data modeling module for generating star schema and 3NF models.
"""
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import re
from datetime import datetime


class DataModel:
    """Represents a data model with tables, relationships, and metadata."""
    
    def __init__(self):
        self.tables: Dict[str, Dict[str, Any]] = {}
        self.relationships: List[Dict[str, Any]] = []
        self.metadata: Dict[str, Any] = {}
    
    def add_table(self, name: str, table_type: str, columns: List[Dict[str, Any]], 
                  primary_key: List[str], grain: Optional[str] = None):
        """Add a table to the model."""
        self.tables[name] = {
            'name': name,
            'type': table_type,  # 'FACT' or 'DIM'
            'columns': columns,
            'primary_key': primary_key,
            'grain': grain,
            'scd_type': 'Type 1' if table_type == 'DIM' else None,
            'clustering_keys': [],
            'metadata_columns': ['LOAD_TS', 'SOURCE_FILE_NAME', 'ROW_HASH', 'RECORD_SOURCE']
        }
    
    def add_relationship(self, from_table: str, to_table: str, from_column: str, 
                        to_column: str, relationship_type: str = 'many_to_one'):
        """Add a relationship between tables."""
        self.relationships.append({
            'from_table': from_table,
            'to_table': to_table,
            'from_column': from_column,
            'to_column': to_column,
            'type': relationship_type
        })


def infer_data_model(df: pd.DataFrame, profile: Dict[str, Any]) -> DataModel:
    """
    Infer a data model from the DataFrame and profiling results.
    
    Returns:
        DataModel object
    """
    model = DataModel()
    entities = profile['entities']
    candidate_keys = profile['candidate_keys']
    grain = profile['grain']
    
    # Determine if we should use star schema or 3NF
    has_facts = len(entities['facts']) > 0
    has_dimensions = len(entities['dimensions']) > 0
    
    if has_facts and has_dimensions:
        # Star schema approach
        model = _build_star_schema(df, profile, model)
    else:
        # 3NF approach - normalize the data
        model = _build_3nf_schema(df, profile, model)
    
    # Add date dimension if dates exist
    if entities['dates']:
        model = _add_date_dimension(model, entities['dates'])
    
    return model


def _build_star_schema(df: pd.DataFrame, profile: Dict[str, Any], model: DataModel) -> DataModel:
    """Build a star schema model."""
    entities = profile['entities']
    candidate_keys = profile['candidate_keys']
    column_types = profile['column_types']
    
    # Identify fact table grain
    fact_key = None
    if candidate_keys:
        fact_key = candidate_keys[0]['columns'][0]  # Use best candidate key
    
    # Create fact table
    fact_columns = []
    fact_pk = []
    
    # Add fact key
    if fact_key:
        fact_columns.append({
            'name': fact_key,
            'type': _map_pandas_to_snowflake_type(df[fact_key].dtype),
            'nullable': df[fact_key].isna().any(),
            'is_pk': True,
            'is_fk': False
        })
        fact_pk.append(fact_key)
    else:
        # Create surrogate key
        fact_columns.append({
            'name': 'FACT_SK',
            'type': 'TEXT',  # SHA256 hex string; use TEXT not NUMBER
            'nullable': False,
            'is_pk': True,
            'is_fk': False
        })
        fact_pk.append('FACT_SK')
    
    # Track column names already added (avoid duplicates, e.g. fact_key also in facts)
    fact_col_names = {c['name'] for c in fact_columns}
    
    # Add fact measures (skip if already added as fact_key)
    for col in entities['facts']:
        if col in fact_col_names:
            continue
        fact_col_names.add(col)
        fact_columns.append({
            'name': col,
            'type': _map_pandas_to_snowflake_type(df[col].dtype),
            'nullable': df[col].isna().any(),
            'is_pk': False,
            'is_fk': False
        })
    
    # Add date columns as FKs to date dimension
    for col in entities['dates']:
        fact_columns.append({
            'name': f'{col}_FK',
            'type': 'TEXT',  # References DATE_SK (TEXT); use TEXT not NUMBER
            'nullable': df[col].isna().any(),
            'is_pk': False,
            'is_fk': True,
            'references': 'DIM_DATE',
            'references_column': 'DATE_SK'
        })
    
    # Create dimension tables first to identify FK columns needed
    dim_groups = _group_dimension_columns(df, entities['dimensions'], entities['ids'])
    
    # Track dimension FK mappings
    dim_fk_mappings = {}  # {dim_name: {'fk_col': '...', 'natural_key': '...'}}
    
    # First pass: identify dimensions and their natural keys
    for dim_name, dim_cols in dim_groups.items():
        natural_key_cols = []
        
        # Find natural key
        for col in dim_cols:
            if col in entities['ids'] or re.match(r'.*_id$', col.lower()):
                natural_key_cols.append(col)
        
        if not natural_key_cols and dim_cols:
            natural_key_cols = [dim_cols[0]]  # Use first column as natural key
        
        if natural_key_cols:
            # Add FK column to fact table (skip if same column already added as fact_key or earlier FK)
            fk_col_name = f'{natural_key_cols[0]}_FK'
            if natural_key_cols[0] in fact_col_names:
                # Natural key already in fact table (e.g. as fact_key) - use it as FK, don't add duplicate
                dim_fk_mappings[dim_name] = {'fk_col': natural_key_cols[0], 'natural_key': natural_key_cols[0]}
            elif fk_col_name in fact_col_names:
                dim_fk_mappings[dim_name] = {'fk_col': fk_col_name, 'natural_key': natural_key_cols[0]}
            else:
                fact_col_names.add(fk_col_name)
                fact_columns.append({
                    'name': fk_col_name,
                    'type': 'TEXT',  # References dim surrogate key (SHA256 hex); use TEXT not NUMBER
                    'nullable': df[natural_key_cols[0]].isna().any() if natural_key_cols[0] in df.columns else True,
                    'is_pk': False,
                    'is_fk': True,
                    'references': dim_name,
                    'references_column': f'{dim_name}_SK'
                })
                dim_fk_mappings[dim_name] = {
                    'fk_col': fk_col_name,
                    'natural_key': natural_key_cols[0]
                }
    
    # Add metadata columns
    for meta_col in ['LOAD_TS', 'SOURCE_FILE_NAME', 'ROW_HASH', 'RECORD_SOURCE']:
        fact_columns.append({
            'name': meta_col,
            'type': 'TIMESTAMP_NTZ' if meta_col == 'LOAD_TS' else 'TEXT',
            'nullable': False if meta_col == 'LOAD_TS' else True,
            'is_pk': False,
            'is_fk': False
        })
    
    model.add_table('FACT_MAIN', 'FACT', fact_columns, fact_pk, grain=profile['grain'])
    
    # Second pass: create dimension tables and relationships
    for dim_name, dim_cols in dim_groups.items():
        dim_columns = []
        dim_pk = []
        natural_key_cols = []
        
        # Find natural key
        for col in dim_cols:
            if col in entities['ids'] or re.match(r'.*_id$', col.lower()):
                natural_key_cols.append(col)
        
        if not natural_key_cols and dim_cols:
            natural_key_cols = [dim_cols[0]]  # Use first column as natural key
        
        # Add surrogate key (SHA256 hex string; use TEXT not NUMBER)
        dim_columns.append({
            'name': f'{dim_name}_SK',
            'type': 'TEXT',
            'nullable': False,
            'is_pk': True,
            'is_fk': False
        })
        dim_pk.append(f'{dim_name}_SK')
        
        # Add natural key
        for nk_col in natural_key_cols:
            dim_columns.append({
                'name': f'{nk_col}_NK',
                'type': _map_pandas_to_snowflake_type(df[nk_col].dtype),
                'nullable': False,
                'is_pk': False,
                'is_fk': False
            })
        
        # Add other dimension attributes
        for col in dim_cols:
            if col not in natural_key_cols:
                dim_columns.append({
                    'name': col,
                    'type': _map_pandas_to_snowflake_type(df[col].dtype),
                    'nullable': df[col].isna().any(),
                    'is_pk': False,
                    'is_fk': False
                })
        
        # Add metadata columns
        for meta_col in ['LOAD_TS', 'SOURCE_FILE_NAME', 'ROW_HASH', 'RECORD_SOURCE']:
            dim_columns.append({
                'name': meta_col,
                'type': 'TIMESTAMP_NTZ' if meta_col == 'LOAD_TS' else 'TEXT',
                'nullable': False if meta_col == 'LOAD_TS' else True,
                'is_pk': False,
                'is_fk': False
            })
        
        model.add_table(dim_name, 'DIM', dim_columns, dim_pk)
        
        # Add relationship from fact to dimension
        if dim_name in dim_fk_mappings:
            fk_col = dim_fk_mappings[dim_name]['fk_col']
            model.add_relationship('FACT_MAIN', dim_name, fk_col, f'{dim_name}_SK')
    
    return model


def _build_3nf_schema(df: pd.DataFrame, profile: Dict[str, Any], model: DataModel) -> DataModel:
    """Build a 3NF normalized model."""
    # For 3NF, create one main table and normalize repeating groups
    candidate_keys = profile['candidate_keys']
    column_types = profile['column_types']
    
    # Main table
    main_columns = []
    main_pk = []
    
    if candidate_keys:
        pk_col = candidate_keys[0]['columns'][0]
        main_pk.append(pk_col)
        main_columns.append({
            'name': pk_col,
            'type': _map_pandas_to_snowflake_type(df[pk_col].dtype),
            'nullable': False,
            'is_pk': True,
            'is_fk': False
        })
    else:
        main_pk.append('MAIN_SK')
        main_columns.append({
            'name': 'MAIN_SK',
            'type': 'TEXT',  # Surrogate key (hash); use TEXT not NUMBER
            'nullable': False,
            'is_pk': True,
            'is_fk': False
        })
    
    # Add all other columns
    for col in df.columns:
        if col not in main_pk:
            main_columns.append({
                'name': col,
                'type': _map_pandas_to_snowflake_type(df[col].dtype),
                'nullable': df[col].isna().any(),
                'is_pk': False,
                'is_fk': False
            })
    
    # Add metadata columns
    for meta_col in ['LOAD_TS', 'SOURCE_FILE_NAME', 'ROW_HASH', 'RECORD_SOURCE']:
        main_columns.append({
            'name': meta_col,
            'type': 'TIMESTAMP_NTZ' if meta_col == 'LOAD_TS' else 'TEXT',
            'nullable': False if meta_col == 'LOAD_TS' else True,
            'is_pk': False,
            'is_fk': False
        })
    
    model.add_table('MAIN_TABLE', 'FACT', main_columns, main_pk)
    
    return model


def _group_dimension_columns(df: pd.DataFrame, dimension_cols: List[str], 
                            id_cols: List[str]) -> Dict[str, List[str]]:
    """Group dimension columns into logical dimension tables."""
    groups = {}
    
    # Group by common prefixes
    prefix_groups = {}
    for col in dimension_cols:
        # Extract prefix (e.g., 'customer_' from 'customer_name')
        parts = col.split('_')
        if len(parts) > 1:
            prefix = '_'.join(parts[:-1])
        else:
            prefix = 'dimension'
        
        if prefix not in prefix_groups:
            prefix_groups[prefix] = []
        prefix_groups[prefix].append(col)
    
    # Create dimension names
    for prefix, cols in prefix_groups.items():
        dim_name = f'DIM_{prefix.upper()}'
        groups[dim_name] = cols
    
    # Add ID columns to appropriate dimensions
    for id_col in id_cols:
        # Try to match to existing dimension
        matched = False
        for dim_name, cols in groups.items():
            if any(id_col.replace('_id', '') in col.lower() for col in cols):
                groups[dim_name].append(id_col)
                matched = True
                break
        
        if not matched:
            # Create new dimension
            entity_name = id_col.replace('_id', '').upper()
            groups[f'DIM_{entity_name}'] = [id_col]
    
    return groups


def _add_date_dimension(model: DataModel, date_columns: List[str]) -> DataModel:
    """Add a date dimension table."""
    date_columns_list = [
        {
            'name': 'DATE_SK',
            'type': 'TEXT',  # Surrogate key; use TEXT for consistency with other SKs
            'nullable': False,
            'is_pk': True,
            'is_fk': False
        },
        {
            'name': 'DATE_NK',
            'type': 'DATE',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        },
        {
            'name': 'YEAR',
            'type': 'NUMBER(38,0)',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        },
        {
            'name': 'QUARTER',
            'type': 'NUMBER(38,0)',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        },
        {
            'name': 'MONTH',
            'type': 'NUMBER(38,0)',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        },
        {
            'name': 'DAY',
            'type': 'NUMBER(38,0)',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        },
        {
            'name': 'DAY_OF_WEEK',
            'type': 'NUMBER(38,0)',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        },
        {
            'name': 'DAY_NAME',
            'type': 'TEXT',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        },
        {
            'name': 'MONTH_NAME',
            'type': 'TEXT',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        },
        {
            'name': 'IS_WEEKEND',
            'type': 'BOOLEAN',
            'nullable': False,
            'is_pk': False,
            'is_fk': False
        }
    ]
    
    # Add metadata columns
    for meta_col in ['LOAD_TS', 'SOURCE_FILE_NAME', 'ROW_HASH', 'RECORD_SOURCE']:
        date_columns_list.append({
            'name': meta_col,
            'type': 'TIMESTAMP_NTZ' if meta_col == 'LOAD_TS' else 'TEXT',
            'nullable': False if meta_col == 'LOAD_TS' else True,
            'is_pk': False,
            'is_fk': False
        })
    
    model.add_table('DIM_DATE', 'DIM', date_columns_list, ['DATE_SK'])
    
    return model


def _map_pandas_to_snowflake_type(dtype) -> str:
    """Map pandas dtype to Snowflake data type."""
    dtype_str = str(dtype).lower()
    
    if 'int' in dtype_str:
        return 'NUMBER(38,0)'
    elif 'float' in dtype_str:
        return 'FLOAT'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    elif 'datetime' in dtype_str or 'date' in dtype_str:
        return 'TIMESTAMP_NTZ'
    else:
        return 'TEXT'


def get_create_table_statements(model: DataModel) -> List[str]:
    """
    Return a list of complete CREATE TABLE statements (one per table).
    Deduplicates columns by name (keeps first occurrence) to avoid SQL duplicate column errors.
    """
    statements = []
    for table_name, table_def in model.tables.items():
        seen_names = set()
        column_defs = []
        for col in table_def['columns']:
            name = col['name']
            if name in seen_names:
                continue
            seen_names.add(name)
            col_def = f"    {name} {col['type']}"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            column_defs.append(col_def)
        sql = f"CREATE OR REPLACE TABLE {table_name} (\n" + ",\n".join(column_defs) + "\n)"
        statements.append(sql)
    return statements


def generate_snowflake_ddl(model: DataModel, database: str, schema: str) -> str:
    """
    Generate Snowflake DDL statements for the model.
    
    Returns:
        SQL DDL string
    """
    ddl_lines = [
        f"-- Data Model DDL for {database}.{schema}",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
        f"USE DATABASE {database};",
        f"USE SCHEMA {schema};",
        ""
    ]
    
    # Create tables
    for table_name, table_def in model.tables.items():
        ddl_lines.append(f"-- Table: {table_name} ({table_def['type']})")
        
        # Document primary key in comments
        pk_cols = table_def.get('primary_key', [])
        if pk_cols:
            pk_cols_str = ", ".join(pk_cols)
            ddl_lines.append(f"-- Primary Key: {pk_cols_str}")
            ddl_lines.append(f"-- Note: Snowflake doesn't enforce PRIMARY KEY constraints")
            ddl_lines.append("")
        
        ddl_lines.append(f"CREATE OR REPLACE TABLE {table_name} (")
        
        column_defs = []
        for col in table_def['columns']:
            col_def = f"    {col['name']} {col['type']}"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            column_defs.append(col_def)
        
        ddl_lines.append(",\n".join(column_defs))
        ddl_lines.append(");")
        ddl_lines.append("")
        
        # Add clustering key for large fact tables (optional, non-fatal)
        if table_def['type'] == 'FACT' and table_def.get('clustering_keys'):
            cluster_cols = ", ".join(table_def['clustering_keys'])
            ddl_lines.append(
                f"-- Clustering key (optional): ALTER TABLE {table_name} CLUSTER BY ({cluster_cols});"
            )
            ddl_lines.append("")
    
    return "\n".join(ddl_lines)


def generate_mermaid_erd(model: DataModel) -> str:
    """
    Generate Mermaid ERD diagram.
    
    Returns:
        Mermaid diagram string
    """
    mermaid_lines = [
        "erDiagram",
        ""
    ]
    
    # Add tables
    for table_name, table_def in model.tables.items():
        mermaid_lines.append(f"    {table_name} {{")
        for col in table_def['columns'][:10]:  # Limit columns for readability
            col_type = col['type'].replace('NUMBER(38,0)', 'INT').replace('TEXT', 'STRING')
            pk_marker = " PK" if col.get('is_pk') else ""
            fk_marker = " FK" if col.get('is_fk') else ""
            mermaid_lines.append(f"        {col['name']} {col_type}{pk_marker}{fk_marker}")
        mermaid_lines.append("    }")
        mermaid_lines.append("")
    
    # Add relationships
    for rel in model.relationships:
        from_table = rel['from_table']
        to_table = rel['to_table']
        mermaid_lines.append(f"    {from_table} ||--o{{ {to_table} : \"{rel['from_column']}\"")
    
    return "\n".join(mermaid_lines)
