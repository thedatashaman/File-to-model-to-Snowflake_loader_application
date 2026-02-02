"""
Snowflake loader module for staging and loading data.
"""
import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas
from typing import Dict, List, Any, Optional
import os
from pathlib import Path
from datetime import datetime
import json
import pandas as pd
from app.core.modeling import DataModel


class SnowflakeLoader:
    """Handles Snowflake connection and data loading."""
    
    def __init__(self, config: Dict[str, str]):
        """
        Initialize Snowflake loader with configuration.
        
        Args:
            config: Dictionary with Snowflake connection parameters
        """
        self.config = config
        self.conn = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Establish connection to Snowflake.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Determine authentication method
            if self.config.get('SNOWFLAKE_PRIVATE_KEY'):
                # Key-pair authentication
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.backends import default_backend
                
                private_key_pem = self.config['SNOWFLAKE_PRIVATE_KEY']
                if private_key_pem.startswith('-----BEGIN'):
                    # Already in PEM format
                    p_key = serialization.load_pem_private_key(
                        private_key_pem.encode('utf-8'),
                        password=None,
                        backend=default_backend()
                    )
                else:
                    # Assume it's a file path
                    with open(private_key_pem, 'rb') as key_file:
                        p_key = serialization.load_pem_private_key(
                            key_file.read(),
                            password=None,
                            backend=default_backend()
                        )
                
                self.conn = snowflake.connector.connect(
                    account=self.config['SNOWFLAKE_ACCOUNT'],
                    user=self.config['SNOWFLAKE_USER'],
                    private_key=p_key,
                    warehouse=self.config.get('SNOWFLAKE_WAREHOUSE'),
                    database=self.config.get('SNOWFLAKE_DATABASE'),
                    schema=self.config.get('SNOWFLAKE_SCHEMA'),
                    role=self.config.get('SNOWFLAKE_ROLE')
                )
            else:
                # Password authentication
                self.conn = snowflake.connector.connect(
                    account=self.config['SNOWFLAKE_ACCOUNT'],
                    user=self.config['SNOWFLAKE_USER'],
                    password=self.config.get('SNOWFLAKE_PASSWORD', ''),
                    warehouse=self.config.get('SNOWFLAKE_WAREHOUSE'),
                    database=self.config.get('SNOWFLAKE_DATABASE'),
                    schema=self.config.get('SNOWFLAKE_SCHEMA'),
                    role=self.config.get('SNOWFLAKE_ROLE')
                )
            
            self.cursor = self.conn.cursor()
            return True
            
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False
    
    def disconnect(self):
        """Close Snowflake connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def database_exists(self, database: str) -> bool:
        """
        Check if database exists using INFORMATION_SCHEMA (reliable across Snowflake versions).
        
        Returns:
            True if database exists, False otherwise
        """
        try:
            db_name_escaped = database.replace("'", "''")
            # Use INFORMATION_SCHEMA - column is DATABASE_NAME in Snowflake
            self.cursor.execute("""
                SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES
                WHERE DATABASE_NAME = %s
            """, (database,))
            result = self.cursor.fetchone()
            if result:
                return True
            # Fallback: SHOW DATABASES and use cursor column names (row may be tuple with different order)
            self.cursor.execute(f"SHOW DATABASES LIKE '{db_name_escaped}'")
            results = self.cursor.fetchall()
            desc = self.cursor.description  # list of (name, type_code, ...)
            if desc and results:
                col_names = [d[0].upper() for d in desc] if desc else []
                name_idx = col_names.index('NAME') if 'NAME' in col_names else (col_names.index('DATABASE_NAME') if 'DATABASE_NAME' in col_names else 0)
                for row in results:
                    if row and len(row) > name_idx:
                        val = row[name_idx]
                        if val is not None and str(val).upper() == database.upper():
                            return True
            return False
        except Exception as e:
            print(f"Error checking database existence: {str(e)}")
            return False
    
    def schema_exists(self, database: str, schema: str) -> bool:
        """
        Check if schema exists using INFORMATION_SCHEMA (reliable across Snowflake versions).
        
        Returns:
            True if schema exists, False otherwise
        """
        try:
            db_name_escaped = database.replace("'", "''")
            schema_name_escaped = schema.replace("'", "''")
            # Use INFORMATION_SCHEMA in the given database
            self.cursor.execute(f"USE DATABASE {db_name_escaped}")
            self.cursor.execute("""
                SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME = %s
            """, (schema,))
            result = self.cursor.fetchone()
            if result:
                return True
            # Fallback: SHOW SCHEMAS and use column names
            self.cursor.execute(f"SHOW SCHEMAS LIKE '{schema_name_escaped}'")
            results = self.cursor.fetchall()
            desc = self.cursor.description
            if desc and results:
                col_names = [d[0].upper() for d in desc] if desc else []
                name_idx = col_names.index('NAME') if 'NAME' in col_names else (col_names.index('SCHEMA_NAME') if 'SCHEMA_NAME' in col_names else 0)
                for row in results:
                    if row and len(row) > name_idx:
                        val = row[name_idx]
                        if val is not None and str(val).upper() == schema.upper():
                            return True
            return False
        except Exception as e:
            print(f"Error checking schema existence: {str(e)}")
            return False
    
    def create_database_schema(self, database: str, schema: str) -> Dict[str, Any]:
        """
        Create database and schema if they don't exist, or use existing ones.
        
        Returns:
            Dictionary with status and information about what was created/existed
        """
        result = {
            'success': True,
            'database_existed': False,
            'schema_existed': False,
            'database_created': False,
            'schema_created': False,
            'message': ''
        }
        
        try:
            # Validate input (basic SQL injection prevention)
            if not database or not schema:
                result['success'] = False
                result['message'] = "Database and schema names are required"
                return result
            
            # Check if database exists
            db_exists = self.database_exists(database)
            result['database_existed'] = db_exists
            
            if not db_exists:
                # Create database (using IF NOT EXISTS for safety)
                db_name_escaped = database.replace("'", "''")
                self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name_escaped}")
                result['database_created'] = True
                result['message'] += f"Database '{database}' created. "
            else:
                result['message'] += f"Database '{database}' already exists (reusing). "
            
            # Use database
            db_name_escaped = database.replace("'", "''")
            self.cursor.execute(f"USE DATABASE {db_name_escaped}")
            
            # Check if schema exists
            schema_exists = self.schema_exists(database, schema)
            result['schema_existed'] = schema_exists
            
            if not schema_exists:
                # Create schema (using IF NOT EXISTS for safety)
                schema_name_escaped = schema.replace("'", "''")
                self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name_escaped}")
                result['schema_created'] = True
                result['message'] += f"Schema '{schema}' created. "
            else:
                result['message'] += f"Schema '{schema}' already exists (reusing). "
            
            # Use schema
            schema_name_escaped = schema.replace("'", "''")
            self.cursor.execute(f"USE SCHEMA {schema_name_escaped}")
            
            return result
            
        except Exception as e:
            error_msg = f"Error creating/using database/schema: {str(e)}"
            print(error_msg)
            result['success'] = False
            result['message'] = error_msg
            return result
    
    def create_stage(self, stage_name: str = 'stg_ingest') -> bool:
        """
        Create internal stage for file uploads.
        
        Returns:
            True if successful
        """
        try:
            self.cursor.execute(f"CREATE OR REPLACE STAGE {stage_name}")
            return True
        except Exception as e:
            print(f"Error creating stage: {str(e)}")
            return False
    
    def upload_file_to_stage(self, local_file_path: str, stage_name: str, 
                            stage_path: str = '', auto_compress: bool = False) -> bool:
        """
        Upload file to Snowflake stage.
        
        Args:
            auto_compress: If False, upload as-is so COPY INTO can match .csv. 
                           Snowflake PUT compresses to .gz by default, which would require 
                           COPY INTO to look for .csv.gz.
        
        Returns:
            True if successful
        """
        try:
            # Use absolute path so PUT finds the file regardless of working directory
            abs_path = os.path.abspath(local_file_path)
            if not os.path.exists(abs_path):
                print(f"Error: file not found: {abs_path}")
                return False
            local_file_path_normalized = abs_path.replace('\\', '/')
            # Snowflake docs: Windows use file://C:/path (two slashes); Linux use file:///path (three slashes)
            if os.path.isabs(abs_path) and local_file_path_normalized[1:2] == ':':
                file_url = 'file://' + local_file_path_normalized  # Windows: file://C:/path
            else:
                file_url = 'file:///' + local_file_path_normalized if os.path.isabs(abs_path) else 'file://' + local_file_path_normalized
            
            # PUT command: disable compression so stage has .csv and COPY INTO FILES = ('x.csv') matches
            compress_opt = "AUTO_COMPRESS=FALSE" if not auto_compress else ""
            put_command = f"PUT {file_url} @{stage_name}"
            if compress_opt:
                put_command += f" {compress_opt}"
            if stage_path:
                put_command += f" {stage_path}"
            
            self.cursor.execute(put_command)
            return True
        except Exception as e:
            print(f"Error uploading file: {str(e)}")
            return False
    
    def create_tables_from_model(self, model: DataModel, database: str, schema: str) -> bool:
        """
        Create tables in Snowflake first (one CREATE TABLE per table), then data load can proceed.
        Uses individual CREATE TABLE statements so tables are created reliably before COPY INTO.
        """
        try:
            from app.core.modeling import get_create_table_statements
            
            db_name_escaped = database.replace("'", "''")
            schema_name_escaped = schema.replace("'", "''")
            
            self.cursor.execute(f"USE DATABASE {db_name_escaped}")
            self.cursor.execute(f"USE SCHEMA {schema_name_escaped}")
            
            # Get one CREATE TABLE statement per table (no splitting, no ALTER)
            statements = get_create_table_statements(model)
            
            for i, stmt in enumerate(statements):
                try:
                    self.cursor.execute(stmt)
                    table_name = list(model.tables.keys())[i] if i < len(model.tables) else f"table_{i}"
                    print(f"Created table: {table_name}")
                except Exception as e:
                    print(f"Error creating table (statement {i + 1}): {str(e)}")
                    print(f"Statement: {stmt[:200]}...")
                    raise
            
            # Optional verification (non-fatal): ensure we're in right context for COPY INTO
            try:
                # Use unquoted identifier so Snowflake resolves schema correctly (e.g. PUBLIC)
                self.cursor.execute(f"USE DATABASE {db_name_escaped}")
                self.cursor.execute(f"USE SCHEMA {schema_name_escaped}")
                self.cursor.execute(f"SHOW TABLES IN SCHEMA {db_name_escaped}.{schema_name_escaped}")
                created = self.cursor.fetchall()
                print(f"Tables in schema: {len(created) if created else 0}")
            except Exception as verify_err:
                # Don't fail: tables were already created; verification is optional
                print(f"Note: table verification skipped ({verify_err})")
            
            return True
        except Exception as e:
            print(f"Error creating tables: {str(e)}")
            return False
    
    def copy_into_table(self, table_name: str, stage_name: str, 
                       file_pattern: str, file_format: str = 'csv',
                       database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Load data from stage into table using COPY INTO.
        
        Returns:
            Dictionary with load results
        """
        try:
            # File format options (avoid embedded quotes that can break ON_ERROR parsing)
            if file_format.lower() == 'csv':
                format_options = "SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"'"
            elif file_format.lower() == 'json':
                format_options = "STRIP_OUTER_ARRAY=TRUE"
            else:
                format_options = ""
            
            # Use fully qualified table name; Snowflake stores unquoted identifiers as UPPERCASE
            if database and schema:
                qualified_table = f'"{database.upper()}"."{schema.upper()}"."{table_name}"'
            else:
                qualified_table = table_name
            
            if database and schema:
                qualified_stage = f'"{database.upper()}"."{schema.upper()}".stg_ingest'
            else:
                qualified_stage = stage_name
            
            # Build COPY INTO; keep ON_ERROR as literal to avoid quote/parse issues
            copy_sql = (
                f"COPY INTO {qualified_table} "
                f"FROM @{qualified_stage} "
                f"FILES = ('{file_pattern}') "
                f"FILE_FORMAT = (TYPE = '{file_format.upper()}' {format_options}) "
                "ON_ERROR = 'CONTINUE'"
            )
            self.cursor.execute(copy_sql)
            
            # Get load results - COPY INTO returns status information
            # Try to get the result, but it may vary by Snowflake version
            try:
                result = self.cursor.fetchone()
                if result:
                    # Result format: [file, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error, first_error_line, first_error_character, first_error_column_name]
                    rows_loaded = result[3] if len(result) > 3 else 0
                    rows_parsed = result[2] if len(result) > 2 else 0
                    errors_seen = result[5] if len(result) > 5 else 0
                else:
                    rows_loaded = 0
                    rows_parsed = 0
                    errors_seen = 0
            except:
                # If fetch fails, try to get row count from table
                try:
                    count_table = qualified_table if (database and schema) else table_name
                    self.cursor.execute(f"SELECT COUNT(*) FROM {count_table}")
                    count_result = self.cursor.fetchone()
                    rows_loaded = count_result[0] if count_result else 0
                    rows_parsed = rows_loaded
                    errors_seen = 0
                except:
                    rows_loaded = 0
                    rows_parsed = 0
                    errors_seen = 0
            
            return {
                'success': True,
                'rows_loaded': rows_loaded,
                'rows_parsed': rows_parsed,
                'errors_seen': errors_seen
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'rows_loaded': 0
            }
    
    def load_table_from_csv(self, file_path: str, table_name: str,
                            database: str, schema: str) -> Dict[str, Any]:
        """
        Load a table directly from a local CSV using write_pandas (bypasses PUT/COPY).
        Use when COPY INTO loads 0 rows (e.g. PUT path issues from Python connector).
        """
        try:
            abs_path = os.path.abspath(file_path)
            if not os.path.exists(abs_path):
                return {'success': False, 'error': f'File not found: {abs_path}', 'rows_loaded': 0}
            
            df = pd.read_csv(abs_path)
            if df.empty:
                return {'success': True, 'rows_loaded': 0}
            
            # Ensure we're in the right database/schema
            db_escaped = database.replace("'", "''")
            schema_escaped = schema.replace("'", "''")
            self.cursor.execute(f"USE DATABASE {db_escaped}")
            self.cursor.execute(f"USE SCHEMA {schema_escaped}")
            
            # write_pandas: use uppercase so we target PUBLIC (Snowflake stores unquoted as UPPERCASE)
            success, nchunks, nrows, _ = write_pandas(
                self.conn, df, table_name,
                database=database.upper(), schema=schema.upper(),
                auto_create_table=False, overwrite=False
            )
            return {
                'success': success,
                'rows_loaded': nrows if success else 0,
                'error': None if success else 'write_pandas failed'
            }
        except Exception as e:
            return {'success': False, 'error': str(e), 'rows_loaded': 0}
    
    def validate_row_counts(self, table_name: str, expected_count: int) -> Dict[str, Any]:
        """
        Validate that table has expected row count.
        
        Returns:
            Dictionary with validation results
        """
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = self.cursor.fetchone()
            actual_count = result[0] if result else 0
            
            return {
                'passed': actual_count == expected_count,
                'expected': expected_count,
                'actual': actual_count,
                'difference': actual_count - expected_count
            }
        except Exception as e:
            return {
                'passed': False,
                'error': str(e)
            }
    
    def create_audit_tables(self, database: str, schema: str) -> bool:
        """
        Create audit/logging tables for ingestion tracking.
        
        Args:
            database: Database name (must be set in context)
            schema: Schema name (must be set in context)
        """
        try:
            # Ensure we're using the correct database and schema
            db_name_escaped = database.replace("'", "''")
            schema_name_escaped = schema.replace("'", "''")
            
            self.cursor.execute(f"USE DATABASE {db_name_escaped}")
            self.cursor.execute(f"USE SCHEMA {schema_name_escaped}")
            
            # Ingestion runs table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS INGESTION_RUNS (
                    RUN_ID NUMBER(38,0) PRIMARY KEY,
                    RUN_START_TS TIMESTAMP_NTZ,
                    RUN_END_TS TIMESTAMP_NTZ,
                    STATUS VARCHAR(50),
                    SOURCE_FILE_NAME VARCHAR(500),
                    TOTAL_TABLES NUMBER(38,0),
                    TABLES_LOADED NUMBER(38,0),
                    TABLES_FAILED NUMBER(38,0),
                    ERROR_MESSAGE VARCHAR(10000)
                )
            """)
            
            # Table status table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS INGESTION_TABLE_STATUS (
                    STATUS_ID NUMBER(38,0) PRIMARY KEY,
                    RUN_ID NUMBER(38,0),
                    TABLE_NAME VARCHAR(255),
                    STATUS VARCHAR(50),
                    ROWS_LOADED NUMBER(38,0),
                    ROWS_EXPECTED NUMBER(38,0),
                    LOAD_START_TS TIMESTAMP_NTZ,
                    LOAD_END_TS TIMESTAMP_NTZ,
                    ERROR_MESSAGE VARCHAR(10000)
                )
            """)
            
            # Errors table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS INGESTION_ERRORS (
                    ERROR_ID NUMBER(38,0) PRIMARY KEY,
                    RUN_ID NUMBER(38,0),
                    TABLE_NAME VARCHAR(255),
                    ROW_NUMBER NUMBER(38,0),
                    ERROR_MESSAGE VARCHAR(10000),
                    REJECTED_ROW VARIANT
                )
            """)
            
            return True
        except Exception as e:
            print(f"Error creating audit tables: {str(e)}")
            return False
    
    def log_ingestion_run(self, run_data: Dict[str, Any]) -> Optional[int]:
        """
        Log an ingestion run.
        
        Returns:
            Run ID if successful
        """
        try:
            # Use a sequence for auto-increment (Snowflake doesn't support AUTOINCREMENT in same way)
            # First check if sequence exists, create if not
            try:
                self.cursor.execute("CREATE SEQUENCE IF NOT EXISTS INGESTION_RUN_ID_SEQ START = 1 INCREMENT = 1")
            except:
                pass  # Sequence might already exist
            
            # Get next value from sequence
            self.cursor.execute("SELECT INGESTION_RUN_ID_SEQ.NEXTVAL")
            run_id_result = self.cursor.fetchone()
            run_id = run_id_result[0] if run_id_result else None
            
            if run_id is None:
                # Fallback: query max ID and add 1
                self.cursor.execute("SELECT COALESCE(MAX(RUN_ID), 0) + 1 FROM INGESTION_RUNS")
                run_id_result = self.cursor.fetchone()
                run_id = run_id_result[0] if run_id_result else 1
            
            self.cursor.execute("""
                INSERT INTO INGESTION_RUNS 
                (RUN_ID, RUN_START_TS, RUN_END_TS, STATUS, SOURCE_FILE_NAME, 
                 TOTAL_TABLES, TABLES_LOADED, TABLES_FAILED, ERROR_MESSAGE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id,
                run_data.get('run_start_ts'),
                run_data.get('run_end_ts'),
                run_data.get('status'),
                run_data.get('source_file_name'),
                run_data.get('total_tables', 0),
                run_data.get('tables_loaded', 0),
                run_data.get('tables_failed', 0),
                run_data.get('error_message')
            ))
            
            return run_id
            
        except Exception as e:
            print(f"Error logging ingestion run: {str(e)}")
            return None
    
    def log_table_status(self, run_id: int, table_data: Dict[str, Any]) -> bool:
        """Log status for a specific table load."""
        try:
            # Get next status ID
            self.cursor.execute("SELECT COALESCE(MAX(STATUS_ID), 0) + 1 FROM INGESTION_TABLE_STATUS")
            status_id_result = self.cursor.fetchone()
            status_id = status_id_result[0] if status_id_result else 1
            
            self.cursor.execute("""
                INSERT INTO INGESTION_TABLE_STATUS
                (STATUS_ID, RUN_ID, TABLE_NAME, STATUS, ROWS_LOADED, ROWS_EXPECTED,
                 LOAD_START_TS, LOAD_END_TS, ERROR_MESSAGE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                status_id,
                run_id,
                table_data.get('table_name'),
                table_data.get('status'),
                table_data.get('rows_loaded', 0),
                table_data.get('rows_expected', 0),
                table_data.get('load_start_ts'),
                table_data.get('load_end_ts'),
                table_data.get('error_message')
            ))
            return True
        except Exception as e:
            print(f"Error logging table status: {str(e)}")
            return False
    
    def load_all_tables(self, model: DataModel, split_files: Dict[str, str],
                       source_file_name: str) -> Dict[str, Any]:
        """
        Load all split files into Snowflake tables.
        
        Returns:
            Dictionary with load results
        """
        run_start = datetime.now()
        results = {
            'run_id': None,
            'tables': {},
            'overall_success': True,
            'errors': []
        }
        
        # Create database/schema FIRST (before audit tables)
        database = self.config.get('SNOWFLAKE_DATABASE', '')
        schema = self.config.get('SNOWFLAKE_SCHEMA', '')
        db_schema_result = self.create_database_schema(database, schema)
        if not db_schema_result['success']:
            results['errors'].append(f"Failed to create/use database/schema: {db_schema_result.get('message', 'Unknown error')}")
            results['overall_success'] = False
            return results
        
        # Store info about what existed/was created
        results['database_info'] = {
            'existed': db_schema_result['database_existed'],
            'created': db_schema_result['database_created']
        }
        results['schema_info'] = {
            'existed': db_schema_result['schema_existed'],
            'created': db_schema_result['schema_created']
        }
        
        # Create audit tables (now that database/schema are set)
        if not self.create_audit_tables(database, schema):
            results['errors'].append("Failed to create audit tables")
            # Don't fail completely, just log the error
        
        # Create stage
        stage_name = f"{schema}.stg_ingest"
        if not self.create_stage(stage_name):
            results['errors'].append("Failed to create stage")
            results['overall_success'] = False
            return results
        
        # Create tables (pass database and schema to ensure context)
        if not self.create_tables_from_model(model, database, schema):
            results['errors'].append("Failed to create tables")
            results['overall_success'] = False
            return results
        
        # Create run_id for logging
        run_id = self.log_ingestion_run({
            'run_start_ts': run_start,
            'run_end_ts': None,  # Will update later
            'status': 'IN_PROGRESS',
            'source_file_name': source_file_name,
            'total_tables': len(split_files),
            'tables_loaded': 0,
            'tables_failed': 0,
            'error_message': None
        })
        results['run_id'] = run_id
        
        # Ensure we're in the correct database/schema before COPY INTO (session context can change)
        db_escaped = database.replace("'", "''")
        schema_escaped = schema.replace("'", "''")
        self.cursor.execute(f"USE DATABASE {db_escaped}")
        self.cursor.execute(f"USE SCHEMA {schema_escaped}")
        
        # Upload and load each table
        tables_loaded = 0
        tables_failed = 0
        
        for table_name, file_path in split_files.items():
            table_result = {
                'table_name': table_name,
                'success': False,
                'rows_loaded': 0,
                'error': None,
                'load_start_ts': datetime.now()
            }
            
            try:
                # Upload file to stage
                file_name = os.path.basename(file_path)
                if not self.upload_file_to_stage(file_path, stage_name):
                    table_result['error'] = "Failed to upload file to stage"
                    table_result['load_end_ts'] = datetime.now()
                    tables_failed += 1
                    results['tables'][table_name] = table_result
                    
                    # Log table status
                    if run_id:
                        self.log_table_status(run_id, {
                            'table_name': table_name,
                            'status': 'FAILED',
                            'rows_loaded': 0,
                            'rows_expected': 0,
                            'load_start_ts': table_result['load_start_ts'],
                            'load_end_ts': table_result['load_end_ts'],
                            'error_message': table_result['error']
                        })
                    continue
                
                # Copy into table (use fully qualified name so table is found)
                copy_result = self.copy_into_table(
                    table_name, stage_name, file_name, 'csv',
                    database=database, schema=schema
                )
                
                table_result['load_end_ts'] = datetime.now()
                
                if copy_result['success'] and (copy_result.get('rows_loaded') or 0) > 0:
                    table_result['success'] = True
                    table_result['rows_loaded'] = copy_result['rows_loaded']
                    tables_loaded += 1
                elif copy_result['success'] and (copy_result.get('rows_loaded') or 0) == 0:
                    # COPY succeeded but 0 rows (e.g. PUT path/stage issue from Python) -> load directly from CSV
                    direct_result = self.load_table_from_csv(
                        file_path, table_name, database, schema
                    )
                    if direct_result['success'] and (direct_result.get('rows_loaded') or 0) > 0:
                        table_result['success'] = True
                        table_result['rows_loaded'] = direct_result['rows_loaded']
                        tables_loaded += 1
                    elif direct_result['success']:
                        table_result['success'] = True
                        table_result['rows_loaded'] = 0
                        tables_loaded += 1
                    else:
                        table_result['error'] = direct_result.get('error', 'Direct load failed')
                        tables_failed += 1
                else:
                    table_result['error'] = copy_result.get('error', 'Unknown error')
                    tables_failed += 1
                
            except Exception as e:
                table_result['error'] = str(e)
                table_result['load_end_ts'] = datetime.now()
                tables_failed += 1
            
            results['tables'][table_name] = table_result
            
            # Log this table's status
            if run_id:
                self.log_table_status(run_id, {
                    'table_name': table_name,
                    'status': 'SUCCESS' if table_result['success'] else 'FAILED',
                    'rows_loaded': table_result['rows_loaded'],
                    'rows_expected': 0,  # Could be calculated from file
                    'load_start_ts': table_result['load_start_ts'],
                    'load_end_ts': table_result['load_end_ts'],
                    'error_message': table_result.get('error')
                })
        
        run_end = datetime.now()
        
        # Update ingestion run status
        if run_id:
            # Update the run record
            try:
                self.cursor.execute("""
                    UPDATE INGESTION_RUNS 
                    SET RUN_END_TS = %s,
                        STATUS = %s,
                        TABLES_LOADED = %s,
                        TABLES_FAILED = %s,
                        ERROR_MESSAGE = %s
                    WHERE RUN_ID = %s
                """, (
                    run_end,
                    'SUCCESS' if tables_failed == 0 else 'PARTIAL' if tables_loaded > 0 else 'FAILED',
                    tables_loaded,
                    tables_failed,
                    '; '.join(results['errors']) if results['errors'] else None,
                    run_id
                ))
            except Exception as e:
                print(f"Error updating run status: {str(e)}")
        
        results['overall_success'] = tables_failed == 0
        
        return results
