"""
Data profiling module for analyzing uploaded data files.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import re


def detect_column_types(df: pd.DataFrame) -> Dict[str, str]:
    """
    Detect column types with enhanced logic for dates and IDs.
    
    Returns:
        Dictionary mapping column names to types
    """
    type_map = {}
    
    for col in df.columns:
        # Check for ID patterns
        if re.match(r'.*_id$|^id$|.*guid.*', col.lower()):
            type_map[col] = 'ID'
            continue
        
        # Check for date patterns
        if re.match(r'.*date|.*time|.*timestamp', col.lower()):
            type_map[col] = 'DATE'
            continue
        
        # Check actual data types
        dtype = str(df[col].dtype)
        
        if 'int' in dtype:
            type_map[col] = 'INTEGER'
        elif 'float' in dtype:
            type_map[col] = 'FLOAT'
        elif 'bool' in dtype:
            type_map[col] = 'BOOLEAN'
        elif 'datetime' in dtype or 'date' in dtype:
            type_map[col] = 'DATE'
        else:
            type_map[col] = 'STRING'
    
    return type_map


def profile_column(df: pd.DataFrame, col: str, col_type: str) -> Dict[str, Any]:
    """
    Profile a single column.
    
    Returns:
        Dictionary with profiling metrics
    """
    series = df[col]
    total_rows = len(series)
    non_null = series.notna().sum()
    null_count = total_rows - non_null
    null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
    
    profile = {
        'column': col,
        'type': col_type,
        'total_rows': total_rows,
        'non_null_count': non_null,
        'null_count': null_count,
        'null_percentage': round(null_pct, 2),
        'distinct_count': series.nunique(),
        'distinct_percentage': round((series.nunique() / total_rows * 100) if total_rows > 0 else 0, 2)
    }
    
    # Numeric statistics
    if col_type in ['INTEGER', 'FLOAT']:
        numeric_series = pd.to_numeric(series, errors='coerce')
        profile['min'] = float(numeric_series.min()) if not numeric_series.isna().all() else None
        profile['max'] = float(numeric_series.max()) if not numeric_series.isna().all() else None
        profile['mean'] = float(numeric_series.mean()) if not numeric_series.isna().all() else None
        profile['median'] = float(numeric_series.median()) if not numeric_series.isna().all() else None
        profile['std'] = float(numeric_series.std()) if not numeric_series.isna().all() else None
        
        # Outlier detection (IQR method)
        q1 = numeric_series.quantile(0.25)
        q3 = numeric_series.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = ((numeric_series < lower_bound) | (numeric_series > upper_bound)).sum()
        profile['outlier_count'] = int(outliers)
        profile['outlier_percentage'] = round((outliers / total_rows * 100) if total_rows > 0 else 0, 2)
    
    # Date statistics
    elif col_type == 'DATE':
        date_series = pd.to_datetime(series, errors='coerce')
        valid_dates = date_series.notna().sum()
        profile['valid_date_count'] = int(valid_dates)
        profile['invalid_date_count'] = int(total_rows - valid_dates)
        if valid_dates > 0:
            profile['min_date'] = str(date_series.min())
            profile['max_date'] = str(date_series.max())
    
    # Categorical statistics
    elif col_type == 'STRING':
        # Top N frequent values
        value_counts = series.value_counts().head(10)
        profile['top_values'] = value_counts.to_dict()
        profile['avg_length'] = round(series.astype(str).str.len().mean(), 2) if non_null > 0 else 0
        profile['max_length'] = int(series.astype(str).str.len().max()) if non_null > 0 else 0
    
    # Boolean statistics
    elif col_type == 'BOOLEAN':
        value_counts = series.value_counts()
        profile['value_counts'] = value_counts.to_dict()
    
    return profile


def detect_candidate_keys(df: pd.DataFrame, min_uniqueness: float = 0.95) -> List[Dict[str, Any]]:
    """
    Detect candidate primary keys.
    
    Args:
        df: DataFrame to analyze
        min_uniqueness: Minimum uniqueness ratio to consider as candidate key
    
    Returns:
        List of candidate keys with their metrics
    """
    candidates = []
    total_rows = len(df)
    
    # Single column keys
    for col in df.columns:
        distinct_count = df[col].nunique()
        uniqueness = distinct_count / total_rows if total_rows > 0 else 0
        
        if uniqueness >= min_uniqueness:
            candidates.append({
                'type': 'single',
                'columns': [col],
                'uniqueness': round(uniqueness, 4),
                'distinct_count': distinct_count,
                'null_count': df[col].isna().sum()
            })
    
    # Composite keys (check pairs)
    for i, col1 in enumerate(df.columns):
        for col2 in df.columns[i+1:]:
            composite_key = df[[col1, col2]].apply(lambda x: '|'.join(x.astype(str)), axis=1)
            distinct_count = composite_key.nunique()
            uniqueness = distinct_count / total_rows if total_rows > 0 else 0
            
            if uniqueness >= min_uniqueness:
                candidates.append({
                    'type': 'composite',
                    'columns': [col1, col2],
                    'uniqueness': round(uniqueness, 4),
                    'distinct_count': distinct_count
                })
    
    # Sort by uniqueness descending
    candidates.sort(key=lambda x: x['uniqueness'], reverse=True)
    return candidates


def detect_entities(df: pd.DataFrame, column_types: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Detect entity types: dimensions, facts, IDs.
    
    Returns:
        Dictionary with 'dimensions', 'facts', 'ids', 'dates'
    """
    entities = {
        'dimensions': [],
        'facts': [],
        'ids': [],
        'dates': []
    }
    
    # ID columns
    for col in df.columns:
        if re.match(r'.*_id$|^id$|.*guid.*', col.lower()):
            entities['ids'].append(col)
        elif column_types.get(col) == 'DATE':
            entities['dates'].append(col)
        elif column_types.get(col) in ['INTEGER', 'FLOAT']:
            # Check if it's a measure (fact) or dimension
            col_lower = col.lower()
            if any(term in col_lower for term in ['amount', 'price', 'cost', 'qty', 'quantity', 
                                                   'total', 'sum', 'count', 'dbu', 'usage', 
                                                   'metric', 'value', 'score', 'rate']):
                entities['facts'].append(col)
            else:
                # Could be a dimension key or fact
                if df[col].nunique() / len(df) < 0.1:  # Low cardinality -> dimension
                    entities['dimensions'].append(col)
                else:
                    entities['facts'].append(col)
        elif column_types.get(col) == 'STRING':
            # String columns are typically dimensions
            if df[col].nunique() / len(df) < 0.5:  # Low to medium cardinality
                entities['dimensions'].append(col)
    
    return entities


def detect_grain(df: pd.DataFrame, candidate_keys: List[Dict[str, Any]]) -> str:
    """
    Detect the grain of the data.
    
    Returns:
        Grain description string
    """
    # Check for transaction ID
    for col in df.columns:
        if 'transaction' in col.lower() and 'id' in col.lower():
            return 'transaction'
    
    # Check for event pattern (timestamp + user/entity ID)
    date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
    id_cols = [col for col in df.columns if '_id' in col.lower() or col.lower() == 'id']
    
    if date_cols and id_cols:
        return 'event'
    
    # Default to row-level
    return 'row_level'


def profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Complete profiling of a DataFrame.
    
    Returns:
        Comprehensive profiling dictionary
    """
    column_types = detect_column_types(df)
    profiles = {}
    
    for col in df.columns:
        profiles[col] = profile_column(df, col, column_types.get(col, 'STRING'))
    
    candidate_keys = detect_candidate_keys(df)
    entities = detect_entities(df, column_types)
    grain = detect_grain(df, candidate_keys)
    
    return {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'column_profiles': profiles,
        'column_types': column_types,
        'candidate_keys': candidate_keys,
        'entities': entities,
        'grain': grain,
        'preview': df.head(100).to_dict('records')
    }
