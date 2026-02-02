"""
Utility functions for file handling, validation, and data processing.
"""
import os
import hashlib
import pandas as pd
import json
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
import chardet
import io


def detect_encoding(file_path: str) -> str:
    """Detect file encoding."""
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
        return result.get('encoding', 'utf-8')


def detect_delimiter(file_path: str, encoding: str = 'utf-8') -> str:
    """Detect CSV delimiter."""
    with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
        first_line = f.readline()
        for delimiter in [',', ';', '\t', '|']:
            if delimiter in first_line:
                return delimiter
    return ','


def load_file(file_path: str, file_type: str, encoding: Optional[str] = None, 
              delimiter: Optional[str] = None, has_header: bool = True,
              chunk_size: int = 100000) -> pd.DataFrame:
    """
    Load a file into a pandas DataFrame with chunked processing for large files.
    
    Args:
        file_path: Path to the file
        file_type: Type of file (csv, json, jsonl, parquet, xlsx)
        encoding: File encoding (auto-detected if None)
        delimiter: CSV delimiter (auto-detected if None)
        has_header: Whether file has headers
        chunk_size: Chunk size for large files
    
    Returns:
        DataFrame with the loaded data
    """
    if encoding is None:
        encoding = detect_encoding(file_path)
    
    file_size = os.path.getsize(file_path)
    use_chunks = file_size > 50 * 1024 * 1024  # 50MB threshold
    
    if file_type.lower() == 'csv':
        if delimiter is None:
            delimiter = detect_delimiter(file_path, encoding)
        
        if use_chunks:
            chunks = []
            for chunk in pd.read_csv(file_path, encoding=encoding, delimiter=delimiter,
                                    header=0 if has_header else None, chunksize=chunk_size,
                                    low_memory=False, on_bad_lines='skip'):
                chunks.append(chunk)
            return pd.concat(chunks, ignore_index=True)
        else:
            return pd.read_csv(file_path, encoding=encoding, delimiter=delimiter,
                             header=0 if has_header else None, low_memory=False,
                             on_bad_lines='skip')
    
    elif file_type.lower() == 'json':
        if use_chunks:
            chunks = []
            with open(file_path, 'r', encoding=encoding) as f:
                for chunk in pd.read_json(f, lines=False, chunksize=chunk_size):
                    chunks.append(chunk)
            return pd.concat(chunks, ignore_index=True)
        else:
            return pd.read_json(file_path, encoding=encoding)
    
    elif file_type.lower() == 'jsonl':
        if use_chunks:
            chunks = []
            with open(file_path, 'r', encoding=encoding) as f:
                for chunk in pd.read_json(f, lines=True, chunksize=chunk_size):
                    chunks.append(chunk)
            return pd.concat(chunks, ignore_index=True)
        else:
            return pd.read_json(file_path, lines=True, encoding=encoding)
    
    elif file_type.lower() == 'parquet':
        return pd.read_parquet(file_path)
    
    elif file_type.lower() in ['xlsx', 'xls']:
        return pd.read_excel(file_path, header=0 if has_header else None)
    
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def validate_file(file_path: str, file_type: str) -> Tuple[bool, Optional[str]]:
    """
    Validate file type and basic structure.
    
    Returns:
        (is_valid, error_message)
    """
    if not os.path.exists(file_path):
        return False, "File does not exist"
    
    if file_type.lower() not in ['csv', 'json', 'jsonl', 'parquet', 'xlsx', 'xls']:
        return False, f"Unsupported file type: {file_type}"
    
    try:
        # Try to read a small sample
        if file_type.lower() == 'csv':
            pd.read_csv(file_path, nrows=1)
        elif file_type.lower() == 'json':
            pd.read_json(file_path, nrows=1)
        elif file_type.lower() == 'jsonl':
            pd.read_json(file_path, lines=True, nrows=1)
        elif file_type.lower() == 'parquet':
            pd.read_parquet(file_path, nrows=1)
        elif file_type.lower() in ['xlsx', 'xls']:
            pd.read_excel(file_path, nrows=1)
    except Exception as e:
        return False, f"File validation failed: {str(e)}"
    
    return True, None


def generate_row_hash(row: pd.Series, columns: list) -> str:
    """Generate deterministic hash for a row."""
    values = [str(row[col]) if pd.notna(row[col]) else '' for col in columns]
    hash_input = '|'.join(values)
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def ensure_output_dir(path: str):
    """Ensure output directory exists."""
    Path(path).mkdir(parents=True, exist_ok=True)


def get_file_metadata(file_path: str) -> Dict[str, Any]:
    """Get file metadata."""
    stat = os.stat(file_path)
    return {
        'size_bytes': stat.st_size,
        'size_mb': round(stat.st_size / (1024 * 1024), 2),
        'modified_time': stat.st_mtime
    }
