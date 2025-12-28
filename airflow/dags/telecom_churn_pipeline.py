"""
Telecom Churn ELT Pipeline DAG
- Reads CSV from raw folder
- Loads data as-is into PostgreSQL staging database
- Moves file to processed folder after successful load
- Transforms data: handles missing values and anonymizes PII
- Loads clean data into telecom database

Configuration: airflow/config/variables.yaml
"""

import os
import shutil
import glob
import hashlib
from datetime import datetime, timedelta

import yaml
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


# ============================================
# Load Configuration from YAML
# ============================================
CONFIG_PATH = '/opt/airflow/config/variables.yaml'

def load_config():
    """Load configuration from YAML file"""
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

# Load config at module level
config = load_config()

# Extract configuration values
RAW_DATA_PATH = config['paths']['raw_data']
PROCESSED_DATA_PATH = config['paths']['processed_data']
STAGING_DB_CONN = config['databases']['staging']['connection_string']
TELECOM_DB_CONN = config['databases']['telecom']['connection_string']
STAGING_TABLE = config['databases']['staging']['table_name']
TELECOM_TABLE = config['databases']['telecom']['table_name']
DEFAULT_VALUES = config['default_values']
CHUNKSIZE = config['processing']['chunksize']
SCHEDULE_INTERVAL = config['dag']['schedule_interval']


# Default arguments for the DAG
default_args = {
    'owner': 'telecom-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def anonymize_customer_id(customer_id):
    """
    Anonymize CustomerID using SHA-256 hash.
    Returns first 16 characters of the hash for readability.
    """
    if pd.isna(customer_id):
        return None
    customer_str = str(customer_id)
    hash_object = hashlib.sha256(customer_str.encode())
    return hash_object.hexdigest()[:16]


def check_for_files(**context):
    """Check if there are CSV files to process in the raw folder"""
    csv_files = glob.glob(os.path.join(RAW_DATA_PATH, '*.csv'))
    
    if not csv_files:
        print(f"No CSV files found in {RAW_DATA_PATH}")
        return []
    
    print(f"Found {len(csv_files)} CSV file(s) to process: {csv_files}")
    
    # Push file list to XCom for downstream tasks
    context['ti'].xcom_push(key='csv_files', value=csv_files)
    return csv_files


def load_csv_to_staging(**context):
    """
    Read CSV files using pandas and load data as-is into staging database.
    No data cleaning or transformation is performed.
    """
    # Get file list from XCom
    csv_files = context['ti'].xcom_pull(key='csv_files', task_ids='check_for_files')
    
    if not csv_files:
        print("No files to process")
        return {'status': 'no_files', 'files_processed': 0, 'rows_loaded': 0}
    
    # Create database engine
    engine = create_engine(STAGING_DB_CONN)
    
    total_rows = 0
    files_processed = []
    
    for csv_file in csv_files:
        file_name = os.path.basename(csv_file)
        print(f"Processing file: {file_name}")
        
        # Read CSV file using pandas - no cleaning, load as-is
        df = pd.read_csv(csv_file)
        
        # Add metadata columns
        df['source_file'] = file_name
        df['loaded_at'] = datetime.now()
        
        print(f"File contains {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Load data into staging table
        df.to_sql(
            name=STAGING_TABLE,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=CHUNKSIZE
        )
        
        print(f"Successfully loaded {len(df)} rows from {file_name} into {STAGING_TABLE}")
        
        total_rows += len(df)
        files_processed.append(csv_file)
    
    engine.dispose()
    
    # Push processed files to XCom
    context['ti'].xcom_push(key='files_processed', value=files_processed)
    
    result = {
        'status': 'success',
        'files_processed': len(files_processed),
        'rows_loaded': total_rows
    }
    print(f"Load complete: {result}")
    return result


def move_files_to_processed(**context):
    """
    Move successfully processed files from raw to processed folder.
    Files are renamed with timestamp to avoid overwrites.
    """
    files_processed = context['ti'].xcom_pull(key='files_processed', task_ids='load_csv_to_staging')
    
    if not files_processed:
        print("No files to move")
        return {'status': 'no_files', 'files_moved': 0}
    
    # Ensure processed directory exists
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    
    files_moved = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for src_file in files_processed:
        file_name = os.path.basename(src_file)
        name, ext = os.path.splitext(file_name)
        
        # Add timestamp to filename to prevent overwrites
        new_file_name = f"{name}_{timestamp}{ext}"
        dest_file = os.path.join(PROCESSED_DATA_PATH, new_file_name)
        
        print(f"Moving {src_file} -> {dest_file}")
        shutil.move(src_file, dest_file)
        
        files_moved.append(dest_file)
        print(f"Successfully moved: {file_name} -> {new_file_name}")
    
    result = {
        'status': 'success',
        'files_moved': len(files_moved),
        'destination': PROCESSED_DATA_PATH
    }
    print(f"Move complete: {result}")
    return result


def transform_data(**context):
    """
    Read data from staging database, process missing values and anonymize PII.
    
    Processing steps:
    1. Read raw data from staging table
    2. Handle missing values using default values
    3. Anonymize CustomerID (PII) using SHA-256 hash
    4. Add transformation metadata
    """
    print("Starting data transformation...")
    
    # Create database engine for staging
    staging_engine = create_engine(STAGING_DB_CONN)
    
    # Read data from staging table
    query = f"SELECT * FROM {STAGING_TABLE}"
    df = pd.read_sql(query, staging_engine)
    staging_engine.dispose()
    
    print(f"Read {len(df)} rows from staging table")
    print(f"Columns: {list(df.columns)}")
    
    if df.empty:
        print("No data to transform")
        context['ti'].xcom_push(key='transform_status', value='no_data')
        return {'status': 'no_data', 'rows_transformed': 0}
    
    # =========================================
    # Step 1: Handle Missing Values
    # =========================================
    print("\n--- Handling Missing Values ---")
    
    # Check for missing values before
    missing_before = df.isnull().sum()
    print(f"Missing values before:\n{missing_before[missing_before > 0]}")
    
    # Fill missing values with defaults from config
    for column, default_value in DEFAULT_VALUES.items():
        if column in df.columns:
            missing_count = df[column].isnull().sum()
            if missing_count > 0:
                print(f"Filling {missing_count} missing values in '{column}' with '{default_value}'")
            df[column] = df[column].fillna(default_value)
    
    # Handle any remaining missing values
    # For numeric columns, fill with 0
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # For string columns, fill with 'Unknown'
    string_cols = df.select_dtypes(include=['object']).columns
    df[string_cols] = df[string_cols].fillna('Unknown')
    
    # Check for missing values after
    missing_after = df.isnull().sum()
    print(f"Missing values after: {missing_after.sum()}")
    
    # =========================================
    # Step 2: Anonymize PII (CustomerID)
    # =========================================
    print("\n--- Anonymizing PII ---")
    
    if 'CustomerID' in df.columns:
        print(f"Anonymizing CustomerID for {len(df)} records...")
        # Store original for logging
        sample_original = df['CustomerID'].head(3).tolist()
        
        # Anonymize CustomerID using hash
        df['customer_id_hash'] = df['CustomerID'].apply(anonymize_customer_id)
        
        # Show sample transformation
        sample_hashed = df['customer_id_hash'].head(3).tolist()
        print(f"Sample anonymization: {list(zip(sample_original, sample_hashed))}")
        
        # Drop original CustomerID (PII)
        df = df.drop(columns=['CustomerID'])
        print("Removed original CustomerID column (PII)")
    
    # =========================================
    # Step 3: Add Transformation Metadata
    # =========================================
    df['transformed_at'] = datetime.now()
    df['transformation_version'] = '1.0'
    
    # Remove staging metadata columns if present
    cols_to_remove = ['source_file', 'loaded_at']
    for col in cols_to_remove:
        if col in df.columns:
            df = df.drop(columns=[col])
    
    print(f"\nTransformation complete. Final columns: {list(df.columns)}")
    print(f"Total rows transformed: {len(df)}")
    
    # Push transformed data info to XCom
    context['ti'].xcom_push(key='transform_status', value='success')
    context['ti'].xcom_push(key='rows_transformed', value=len(df))
    
    # Store transformed dataframe for next task
    # Save to temporary parquet file (more efficient than XCom for large data)
    temp_file = '/tmp/transformed_data.parquet'
    df.to_parquet(temp_file, index=False)
    context['ti'].xcom_push(key='temp_data_file', value=temp_file)
    
    return {
        'status': 'success',
        'rows_transformed': len(df),
        'columns': list(df.columns)
    }


def load_to_telecom(**context):
    """
    Load transformed data into telecom database.
    """
    print("Loading transformed data to telecom database...")
    
    # Check transformation status
    transform_status = context['ti'].xcom_pull(key='transform_status', task_ids='transform_data')
    
    if transform_status != 'success':
        print(f"Skipping load - transform status: {transform_status}")
        return {'status': 'skipped', 'reason': 'no data transformed'}
    
    # Read transformed data from temp file
    temp_file = context['ti'].xcom_pull(key='temp_data_file', task_ids='transform_data')
    df = pd.read_parquet(temp_file)
    
    print(f"Loading {len(df)} rows to telecom database...")
    print(f"Columns: {list(df.columns)}")
    
    # Create database engine for telecom
    telecom_engine = create_engine(TELECOM_DB_CONN)
    
    # Load data into telecom table
    df.to_sql(
        name=TELECOM_TABLE,
        con=telecom_engine,
        if_exists='replace',  # Replace to avoid duplicates
        index=False,
        method='multi',
        chunksize=CHUNKSIZE
    )
    
    telecom_engine.dispose()
    
    # Clean up temp file
    if os.path.exists(temp_file):
        os.remove(temp_file)
        print(f"Cleaned up temp file: {temp_file}")
    
    print(f"Successfully loaded {len(df)} rows to {TELECOM_TABLE}")
    
    return {
        'status': 'success',
        'rows_loaded': len(df),
        'table': TELECOM_TABLE,
        'database': 'telecom'
    }


# Define the DAG
with DAG(
    dag_id='telecom_churn_pipeline',
    default_args=default_args,
    description='Telecom Churn ELT Pipeline - Extract, Load, Transform',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2025, 12, 27),
    catchup=False,
    tags=['telecom', 'churn'],
) as dag:
    
    # Start task
    start = EmptyOperator(
        task_id='start',
    )
    
    # Check for files in raw folder
    check_files = PythonOperator(
        task_id='check_for_files',
        python_callable=check_for_files,
    )
    
    # Load CSV to staging database
    load_to_staging = PythonOperator(
        task_id='load_csv_to_staging',
        python_callable=load_csv_to_staging,
    )
    
    # Move processed files
    move_files = PythonOperator(
        task_id='move_files_to_processed',
        python_callable=move_files_to_processed,
    )
    
    # Transform data: handle missing values and anonymize PII
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    # Load to telecom database
    load_telecom = PythonOperator(
        task_id='load_to_telecom',
        python_callable=load_to_telecom,
    )
    
    # End task
    end = EmptyOperator(
        task_id='end',
    )
    
    # Define task dependencies
    start >> check_files >> load_to_staging >> move_files >> transform >> load_telecom >> end
