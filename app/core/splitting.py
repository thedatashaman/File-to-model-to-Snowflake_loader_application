"""
File splitting module for creating dimension and fact table files.
"""
import pandas as pd
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime
import os
from app.core.modeling import DataModel
from app.core.utils import ensure_output_dir, generate_row_hash


def generate_surrogate_key(row: pd.Series, natural_key_cols: List[str]) -> str:
    """
    Generate deterministic surrogate key using SHA256 hash.
    
    Args:
        row: DataFrame row
        natural_key_cols: List of column names that form the natural key
    
    Returns:
        Hexadecimal hash string
    """
    values = []
    for col in natural_key_cols:
        val = str(row[col]) if pd.notna(row[col]) else ''
        values.append(val)
    
    hash_input = '|'.join(values)
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def split_dataframe(df: pd.DataFrame, model: DataModel, source_file_name: str,
                    output_dir: str = 'app/output') -> Dict[str, Any]:
    """
    Split DataFrame into dimension and fact table files based on the model.
    
    Returns:
        Dictionary with file paths and row counts
    """
    ensure_output_dir(output_dir)
    results = {
        'files': {},
        'row_counts': {},
        'errors': []
    }
    
    load_ts = datetime.now()
    
    # Process dimension tables first
    dim_tables = {name: table for name, table in model.tables.items() 
                  if table['type'] == 'DIM'}
    
    dim_mappings = {}  # Store natural key to surrogate key mappings
    
    for dim_name, dim_def in dim_tables.items():
        try:
            dim_df, mapping = _create_dimension_table(df, dim_def, dim_name, 
                                                     source_file_name, load_ts)
            
            # Save dimension file
            output_path = os.path.join(output_dir, f'{dim_name.lower()}.csv')
            dim_df.to_csv(output_path, index=False)
            
            results['files'][dim_name] = output_path
            results['row_counts'][dim_name] = len(dim_df)
            dim_mappings[dim_name] = mapping
            
        except Exception as e:
            results['errors'].append(f"Error creating {dim_name}: {str(e)}")
    
    # Process fact tables
    fact_tables = {name: table for name, table in model.tables.items() 
                   if table['type'] == 'FACT'}
    
    for fact_name, fact_def in fact_tables.items():
        try:
            fact_df = _create_fact_table(df, fact_def, fact_name, dim_mappings,
                                        source_file_name, load_ts)
            
            # Save fact file
            output_path = os.path.join(output_dir, f'{fact_name.lower()}.csv')
            fact_df.to_csv(output_path, index=False)
            
            results['files'][fact_name] = output_path
            results['row_counts'][fact_name] = len(fact_df)
            
        except Exception as e:
            results['errors'].append(f"Error creating {fact_name}: {str(e)}")
    
    return results


def _create_dimension_table(df: pd.DataFrame, dim_def: Dict[str, Any], 
                            dim_name: str, source_file_name: str, 
                            load_ts: datetime) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Create a dimension table with deduplication and surrogate keys.
    
    Returns:
        (dimension DataFrame, natural_key -> surrogate_key mapping)
    """
    # Find natural key columns
    natural_key_cols = []
    for col in dim_def['columns']:
        if col['name'].endswith('_NK'):
            # Extract original column name
            nk_col = col['name'].replace('_NK', '')
            if nk_col in df.columns:
                natural_key_cols.append(nk_col)
    
    if not natural_key_cols:
        # Use first non-SK column as natural key
        for col in dim_def['columns']:
            if not col['name'].endswith('_SK') and col['name'] not in ['LOAD_TS', 'SOURCE_FILE_NAME', 'ROW_HASH', 'RECORD_SOURCE']:
                if col['name'].replace('_NK', '') in df.columns:
                    natural_key_cols.append(col['name'].replace('_NK', ''))
                    break
    
    # Get dimension attribute columns
    dim_attr_cols = []
    for col in dim_def['columns']:
        col_name = col['name']
        if col_name.endswith('_SK'):
            continue
        elif col_name.endswith('_NK'):
            # Map to original column
            orig_col = col_name.replace('_NK', '')
            if orig_col in df.columns:
                dim_attr_cols.append((col_name, orig_col))
        elif col_name not in ['LOAD_TS', 'SOURCE_FILE_NAME', 'ROW_HASH', 'RECORD_SOURCE']:
            if col_name in df.columns:
                dim_attr_cols.append((col_name, col_name))
    
    # Create dimension records
    dim_records = []
    mapping = {}  # natural_key -> surrogate_key
    
    # Group by natural key to deduplicate
    if natural_key_cols:
        grouped = df.groupby(natural_key_cols)
    else:
        # No natural key, use all dimension columns
        grouped = df.groupby([col[1] for col in dim_attr_cols])
    
    for key, group_df in grouped:
        # Get first row from group (representative record)
        row = group_df.iloc[0]
        
        # Generate surrogate key
        if isinstance(key, tuple):
            key_str = '|'.join(str(k) for k in key)
        else:
            key_str = str(key)
        
        surrogate_key = generate_surrogate_key(row, natural_key_cols if natural_key_cols else [col[1] for col in dim_attr_cols])
        
        # Create dimension record
        dim_record = {
            f'{dim_name}_SK': surrogate_key
        }
        
        # Add natural key
        if natural_key_cols:
            for i, nk_col in enumerate(natural_key_cols):
                dim_record[f'{nk_col}_NK'] = row[nk_col] if pd.notna(row[nk_col]) else ''
                mapping[nk_col] = surrogate_key
        
        # Add attributes
        for target_col, source_col in dim_attr_cols:
            dim_record[target_col] = row[source_col] if pd.notna(row[source_col]) else None
        
        # Add metadata
        dim_record['LOAD_TS'] = load_ts
        dim_record['SOURCE_FILE_NAME'] = source_file_name
        dim_record['ROW_HASH'] = generate_row_hash(row, df.columns.tolist())
        dim_record['RECORD_SOURCE'] = source_file_name
        
        dim_records.append(dim_record)
        
        # Store mapping
        if natural_key_cols:
            if len(natural_key_cols) == 1:
                mapping[row[natural_key_cols[0]]] = surrogate_key
            else:
                mapping[key_str] = surrogate_key
    
    dim_df = pd.DataFrame(dim_records)
    return dim_df, mapping


def _create_fact_table(df: pd.DataFrame, fact_def: Dict[str, Any], 
                      fact_name: str, dim_mappings: Dict[str, Dict[str, str]],
                      source_file_name: str, load_ts: datetime) -> pd.DataFrame:
    """
    Create a fact table with foreign keys to dimensions.
    
    Returns:
        Fact table DataFrame
    """
    fact_records = []
    
    # Find primary key column
    pk_col = None
    if fact_def['primary_key']:
        pk_col = fact_def['primary_key'][0]
        if pk_col.endswith('_SK'):
            # Need to generate surrogate key
            pk_col = None
    
    # Get fact columns
    fact_columns = []
    fk_columns = []
    
    for col in fact_def['columns']:
        col_name = col['name']
        if col_name.endswith('_SK') and col.get('is_pk'):
            # Primary key surrogate key
            continue
        elif col_name.endswith('_FK'):
            # Foreign key - need to resolve
            fk_columns.append(col)
        elif col_name not in ['LOAD_TS', 'SOURCE_FILE_NAME', 'ROW_HASH', 'RECORD_SOURCE']:
            if col_name in df.columns:
                fact_columns.append((col_name, col_name))
    
    # Process each row
    for idx, row in df.iterrows():
        fact_record = {}
        
        # Add primary key
        if pk_col and pk_col in df.columns:
            fact_record[pk_col] = row[pk_col]
        elif pk_col is None:
            # Generate surrogate key
            fact_record['FACT_SK'] = generate_surrogate_key(row, df.columns.tolist()[:5])
        
        # Add fact measures
        for target_col, source_col in fact_columns:
            fact_record[target_col] = row[source_col] if pd.notna(row[source_col]) else None
        
        # Resolve foreign keys
        for fk_col in fk_columns:
            # Find which dimension this FK references
            ref_table = fk_col.get('references')
            if ref_table and ref_table in dim_mappings:
                dim_mapping = dim_mappings[ref_table]
                
                # Try to find the natural key column in the current row
                # Extract the natural key column name from FK column name
                # e.g., customer_id_FK -> customer_id
                fk_col_name = fk_col['name'].replace('_FK', '')
                
                # Try different variations
                possible_nk_cols = [
                    fk_col_name,
                    fk_col_name.replace('_id', ''),
                    fk_col_name.replace('_ID', '')
                ]
                
                # Find matching natural key in row
                matched_sk = None
                for nk_col in possible_nk_cols:
                    if nk_col in df.columns:
                        nk_value = row[nk_col]
                        if pd.notna(nk_value) and nk_value in dim_mapping:
                            matched_sk = dim_mapping[nk_value]
                            break
                
                fact_record[fk_col['name']] = matched_sk
        
        # Add metadata
        fact_record['LOAD_TS'] = load_ts
        fact_record['SOURCE_FILE_NAME'] = source_file_name
        fact_record['ROW_HASH'] = generate_row_hash(row, df.columns.tolist())
        fact_record['RECORD_SOURCE'] = source_file_name
        
        fact_records.append(fact_record)
    
    fact_df = pd.DataFrame(fact_records)
    return fact_df
