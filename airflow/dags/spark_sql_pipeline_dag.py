"""
Spark SQL Pipeline DAG
- Orchestrates two SQL files:
  1. Read CSV and load to staging database
  2. Transform and load to production database
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# Configuration
SPARK_MASTER_URL = "spark://spark-master:7077"
SPARK_CONTAINER = "telecom_spark_master"
CONTAINER_RUNTIME = "podman"  # or 'docker'
SQL_DIR = "/opt/airflow/dags"

# SQL file paths
STAGING_SQL_FILE = f"{SQL_DIR}/1.read_and_load_data_in_staging.sql"
TRANSFORM_SQL_FILE = f"{SQL_DIR}/2.transform_and_load_data_in_production.sql"

# Default arguments
default_args = {
    'owner': 'telecom-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def execute_spark_sql_file(sql_file_path: str, **context):
    """
    Execute a Spark SQL file using spark-sql CLI
    
    :param sql_file_path: Path to the SQL file to execute
    """
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
    
    # Build spark-sql command
    cmd = [
        CONTAINER_RUNTIME, 'exec', SPARK_CONTAINER,
        '/opt/spark/bin/spark-sql',
        '--master', SPARK_MASTER_URL,
        '-f', sql_file_path
    ]
    
    print(f"Executing Spark SQL file: {sql_file_path}")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode != 0:
            error_msg = f"Spark SQL execution failed with return code {result.returncode}"
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            raise RuntimeError(f"{error_msg}\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
        
        print("Spark SQL execution successful")
        if result.stdout:
            print(f"Output: {result.stdout}")
        
        return result.stdout
        
    except subprocess.TimeoutExpired:
        error_msg = "Spark SQL execution timed out after 1 hour"
        print(error_msg)
        raise RuntimeError(error_msg)
    except Exception as e:
        print(f"Error executing Spark SQL: {str(e)}")
        raise


def load_to_staging(**context):
    """Execute SQL file to read CSV and load to staging database"""
    return execute_spark_sql_file(STAGING_SQL_FILE, **context)


def transform_and_load_to_production(**context):
    """Execute SQL file to transform and load to production database"""
    return execute_spark_sql_file(TRANSFORM_SQL_FILE, **context)


# Create DAG
with DAG(
    'spark_sql_pipeline',
    default_args=default_args,
    description='Spark SQL Pipeline: Load CSV to staging, then transform and load to production',
    schedule_interval=None,  # Manual trigger or set schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'sql', 'postgresql', 'staging', 'production'],
) as dag:
    
    # Task 1: Load CSV data to staging database
    load_staging = PythonOperator(
        task_id='load_to_staging',
        python_callable=load_to_staging,
    )
    
    # Task 2: Transform and load to production database
    transform_production = PythonOperator(
        task_id='transform_and_load_to_production',
        python_callable=transform_and_load_to_production,
    )
    
    # Define task dependencies
    load_staging >> transform_production
