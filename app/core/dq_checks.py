"""
Data quality checks module.
"""
import pandas as pd
from typing import Dict, List, Any, Optional
from app.core.modeling import DataModel


def check_primary_key_uniqueness(df: pd.DataFrame, pk_columns: List[str]) -> Dict[str, Any]:
    """
    Check if primary key columns are unique.
    
    Returns:
        Dictionary with check results
    """
    if not pk_columns:
        return {
            'passed': False,
            'message': 'No primary key columns specified',
            'duplicate_count': 0
        }
    
    # Check for duplicates
    duplicates = df.duplicated(subset=pk_columns, keep=False)
    duplicate_count = duplicates.sum()
    
    return {
        'passed': duplicate_count == 0,
        'message': f'Found {duplicate_count} duplicate primary key rows' if duplicate_count > 0 else 'Primary key is unique',
        'duplicate_count': int(duplicate_count),
        'duplicate_rows': df[duplicates][pk_columns].to_dict('records') if duplicate_count > 0 else []
    }


def check_foreign_key_integrity(fact_df: pd.DataFrame, dim_df: pd.DataFrame,
                                fk_column: str, pk_column: str) -> Dict[str, Any]:
    """
    Check referential integrity between fact and dimension tables.
    
    Returns:
        Dictionary with check results
    """
    # Get all foreign keys in fact table
    fact_fks = set(fact_df[fk_column].dropna().unique())
    
    # Get all primary keys in dimension table
    dim_pks = set(dim_df[pk_column].unique())
    
    # Find orphaned foreign keys
    orphaned_fks = fact_fks - dim_pks
    orphaned_count = len(orphaned_fks)
    
    return {
        'passed': orphaned_count == 0,
        'message': f'Found {orphaned_count} orphaned foreign keys' if orphaned_count > 0 else 'All foreign keys valid',
        'orphaned_count': orphaned_count,
        'orphaned_keys': list(orphaned_fks)[:100] if orphaned_count > 0 else []  # Limit to 100
    }


def check_null_constraints(df: pd.DataFrame, required_columns: List[str]) -> Dict[str, Any]:
    """
    Check if required columns have nulls.
    
    Returns:
        Dictionary with check results
    """
    violations = {}
    
    for col in required_columns:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                violations[col] = {
                    'null_count': int(null_count),
                    'null_percentage': round((null_count / len(df)) * 100, 2)
                }
    
    return {
        'passed': len(violations) == 0,
        'message': f'Found nulls in {len(violations)} required columns' if violations else 'All required columns have no nulls',
        'violations': violations
    }


def check_data_types(df: pd.DataFrame, expected_types: Dict[str, str]) -> Dict[str, Any]:
    """
    Check if columns match expected data types.
    
    Returns:
        Dictionary with check results
    """
    violations = {}
    
    for col, expected_type in expected_types.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            
            # Simplified type checking
            type_match = False
            if 'int' in expected_type.lower() and 'int' in actual_type.lower():
                type_match = True
            elif 'float' in expected_type.lower() and 'float' in actual_type.lower():
                type_match = True
            elif 'string' in expected_type.lower() or 'text' in expected_type.lower():
                type_match = 'object' in actual_type.lower() or 'string' in actual_type.lower()
            elif 'date' in expected_type.lower() or 'timestamp' in expected_type.lower():
                type_match = 'datetime' in actual_type.lower() or 'date' in actual_type.lower()
            
            if not type_match:
                violations[col] = {
                    'expected': expected_type,
                    'actual': actual_type
                }
    
    return {
        'passed': len(violations) == 0,
        'message': f'Type mismatches in {len(violations)} columns' if violations else 'All columns match expected types',
        'violations': violations
    }


def run_all_dq_checks(model: DataModel, split_files: Dict[str, str]) -> Dict[str, Any]:
    """
    Run all data quality checks on split files.
    
    Returns:
        Dictionary with all check results
    """
    results = {
        'checks': [],
        'overall_passed': True
    }
    
    # Load all files
    loaded_tables = {}
    for table_name, file_path in split_files.items():
        try:
            loaded_tables[table_name] = pd.read_csv(file_path)
        except Exception as e:
            results['checks'].append({
                'table': table_name,
                'check': 'file_load',
                'passed': False,
                'message': f'Failed to load file: {str(e)}'
            })
            results['overall_passed'] = False
            continue
    
    # Run checks for each table
    for table_name, table_def in model.tables.items():
        if table_name not in loaded_tables:
            continue
        
        df = loaded_tables[table_name]
        
        # Primary key uniqueness
        if table_def['primary_key']:
            pk_check = check_primary_key_uniqueness(df, table_def['primary_key'])
            results['checks'].append({
                'table': table_name,
                'check': 'primary_key_uniqueness',
                **pk_check
            })
            if not pk_check['passed']:
                results['overall_passed'] = False
        
        # Null constraints
        required_cols = [col['name'] for col in table_def['columns'] 
                        if not col.get('nullable', True)]
        if required_cols:
            null_check = check_null_constraints(df, required_cols)
            results['checks'].append({
                'table': table_name,
                'check': 'null_constraints',
                **null_check
            })
            if not null_check['passed']:
                results['overall_passed'] = False
    
    # Foreign key integrity checks
    for rel in model.relationships:
        from_table = rel['from_table']
        to_table = rel['to_table']
        
        if from_table in loaded_tables and to_table in loaded_tables:
            fact_df = loaded_tables[from_table]
            dim_df = loaded_tables[to_table]
            
            fk_check = check_foreign_key_integrity(
                fact_df, dim_df,
                rel['from_column'], rel['to_column']
            )
            results['checks'].append({
                'table': f'{from_table} -> {to_table}',
                'check': 'foreign_key_integrity',
                **fk_check
            })
            if not fk_check['passed']:
                results['overall_passed'] = False
    
    return results
