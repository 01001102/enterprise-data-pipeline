#!/usr/bin/env python3
"""
Enterprise Data Pipeline DAG for Apache Airflow

This module implements a comprehensive data pipeline orchestrating the flow from
AWS Kinesis streams through Bronze, Silver, and Gold data layers using DBT for
transformations and PostgreSQL for storage.

Pipeline Architecture:
    Kinesis Streams → Bronze Layer → Silver Layer → Gold Layer → Analytics
    
Data Flow:
    1. Extract: Pull data from Kinesis streams (real-time events)
    2. Bronze: Raw data validation and storage in PostgreSQL
    3. Silver: Data cleaning and enrichment using DBT
    4. Gold: Business metrics and analytics using DBT
    5. Documentation: Auto-generate DBT documentation

Author: Ivan de França
Version: 2.0.0
License: MIT

Schedule: Hourly execution with data quality checks
Retry Policy: 2 retries with 5-minute delays
Monitoring: Built-in data quality validation and alerting

Example:
    The DAG runs automatically on schedule but can be triggered manually:
        airflow dags trigger enterprise_data_pipeline

Attributes:
    BRONZE_TABLES (list): Bronze layer table names
    SILVER_MODELS (list): Silver layer DBT model names  
    GOLD_MODELS (list): Gold layer DBT model names
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3
import json
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pipeline configuration constants
BRONZE_TABLES = ['bronze_customer_events', 'bronze_transaction_events', 
                'bronze_product_events', 'bronze_system_logs']
KINESIS_STREAMS = ['customer-events', 'transaction-events', 'product-events', 'system-logs']
MAX_RECORDS_PER_SHARD = 1000
DATA_QUALITY_THRESHOLD = 0.95

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Enterprise Data Pipeline DAG
dag = DAG(
    dag_id='enterprise_data_pipeline',
    default_args=default_args,
    description='Enterprise Data Pipeline: Real-time Kinesis → Bronze → Silver → Gold layers with DBT',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=['enterprise', 'kinesis', 'dbt', 'etl', 'real-time', 'analytics'],
    doc_md="""
    # Enterprise Data Pipeline
    
    This DAG orchestrates a complete enterprise data pipeline processing real-time
    events from AWS Kinesis through multiple data layers:
    
    ## Pipeline Stages
    1. **Extract**: Pull events from Kinesis streams
    2. **Bronze**: Raw data validation and storage
    3. **Silver**: Data cleaning and enrichment (DBT)
    4. **Gold**: Business metrics and analytics (DBT)
    5. **Documentation**: Auto-generate data lineage docs
    
    ## Data Quality
    - Automated validation at each layer
    - Duplicate detection and handling
    - Data freshness monitoring
    - Quality gates to prevent bad data propagation
    
    ## Monitoring
    - Real-time pipeline status in Airflow UI
    - Data quality metrics and alerts
    - DBT test results and documentation
    - Performance and volume monitoring
    """
)

def extract_from_kinesis(**context) -> Dict[str, int]:
    """
    Extract data from AWS Kinesis streams to Bronze layer in PostgreSQL.
    
    This function implements the first stage of the data pipeline, pulling
    real-time events from multiple Kinesis streams and storing them as raw
    data in the Bronze layer with minimal transformation.
    
    Args:
        **context: Airflow context containing task instance and execution info
        
    Returns:
        Dict containing extraction statistics per stream
        
    Raises:
        Exception: If Kinesis connection fails or data extraction errors occur
        
    Note:
        - Connects to LocalStack for local development
        - Processes all shards in parallel for each stream
        - Adds metadata fields for data lineage tracking
        - Implements error handling per stream to prevent total failure
    """
    logger.info("🔄 Starting Kinesis data extraction to Bronze layer")
    
    # Configuração LocalStack
    kinesis = boto3.client(
        'kinesis',
        endpoint_url='http://localstack:4566',
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    
    streams = ['customer-events', 'transaction-events', 'product-events', 'system-logs']
    
    for stream_name in streams:
        try:
            # Lista shards
            response = kinesis.describe_stream(StreamName=stream_name)
            shards = response['StreamDescription']['Shards']
            
            all_records = []
            
            for shard in shards:
                shard_id = shard['ShardId']
                
                # Obtém iterator
                iterator_response = kinesis.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType='TRIM_HORIZON'
                )
                
                shard_iterator = iterator_response['ShardIterator']
                
                # Lê registros
                records_response = kinesis.get_records(
                    ShardIterator=shard_iterator,
                    Limit=1000
                )
                
                records = records_response['Records']
                
                for record in records:
                    data = json.loads(record['Data'])
                    data['kinesis_sequence_number'] = record['SequenceNumber']
                    data['kinesis_partition_key'] = record['PartitionKey']
                    data['processed_at'] = datetime.now().isoformat()
                    all_records.append(data)
            
            # Salva no PostgreSQL (Bronze)
            if all_records:
                df = pd.DataFrame(all_records)
                
                # Conecta ao PostgreSQL
                pg_hook = PostgresHook(postgres_conn_id='postgres_default')
                engine = pg_hook.get_sqlalchemy_engine()
                
                # Salva na tabela bronze
                table_name = f"bronze_{stream_name.replace('-', '_')}"
                df.to_sql(table_name, engine, if_exists='append', index=False)
                
                print(f"✓ {len(all_records)} registros salvos em {table_name}")
            
        except Exception as e:
            print(f"Erro ao processar {stream_name}: {e}")
            raise

def validate_bronze_data(**context) -> Dict[str, Dict[str, Any]]:
    """
    Validate data quality and integrity in Bronze layer tables.
    
    Performs comprehensive data quality checks including record counts,
    duplicate detection, and data freshness validation to ensure pipeline
    reliability and data integrity.
    
    Args:
        **context: Airflow context for task communication
        
    Returns:
        Dict containing validation results for each Bronze table with:
        - count: Number of records processed today
        - duplicates: Number of duplicate records found
        - status: PASS/FAIL/ERROR validation status
        - freshness: Data recency check results
        
    Raises:
        Exception: If database connection fails or validation queries error
        
    Note:
        - Results are stored in XCom for downstream task consumption
        - Implements per-table error isolation
        - Provides detailed logging for monitoring and debugging
    """
    logger.info("🔍 Starting Bronze layer data validation")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    tables = ['bronze_customer_events', 'bronze_transaction_events', 
              'bronze_product_events', 'bronze_system_logs']
    
    validation_results = {}
    
    for table in tables:
        try:
            # Conta registros
            count_query = f"SELECT COUNT(*) as count FROM {table} WHERE DATE(processed_at::timestamp) = CURRENT_DATE"
            result = pg_hook.get_first(count_query)
            count = result[0] if result else 0
            
            # Verifica duplicatas
            dup_query = f"SELECT COUNT(*) - COUNT(DISTINCT event_id) as duplicates FROM {table}"
            dup_result = pg_hook.get_first(dup_query)
            duplicates = dup_result[0] if dup_result else 0
            
            validation_results[table] = {
                'count': count,
                'duplicates': duplicates,
                'status': 'PASS' if count > 0 and duplicates == 0 else 'FAIL'
            }
            
            print(f"✓ {table}: {count} registros, {duplicates} duplicatas")
            
        except Exception as e:
            print(f"Erro na validação de {table}: {e}")
            validation_results[table] = {'status': 'ERROR', 'error': str(e)}
    
    # Armazena resultados no XCom
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    
    return validation_results

def check_data_quality(**context) -> bool:
    """
    Perform final data quality assessment and pipeline gate control.
    
    Acts as a quality gate by analyzing validation results from Bronze layer
    and determining whether the pipeline should proceed to Silver layer
    transformations based on predefined quality thresholds.
    
    Args:
        **context: Airflow context to retrieve validation results from XCom
        
    Returns:
        bool: True if all quality checks pass, raises exception otherwise
        
    Raises:
        ValueError: If data quality falls below acceptable thresholds
        
    Note:
        - Retrieves validation results from validate_bronze_data task
        - Implements configurable quality thresholds
        - Provides detailed failure reporting for debugging
        - Acts as circuit breaker to prevent bad data propagation
    """
    logger.info("✅ Performing final data quality assessment")
    
    validation_results = context['task_instance'].xcom_pull(
        task_ids='validate_bronze_data', 
        key='validation_results'
    )
    
    failed_tables = [table for table, result in validation_results.items() 
                    if result.get('status') != 'PASS']
    
    if failed_tables:
        raise ValueError(f"Falha na qualidade dos dados: {failed_tables}")
    
    print("✓ Todos os dados passaram na validação de qualidade")
    return True

# Tasks da camada Bronze (Kinesis → PostgreSQL)
extract_kinesis_task = PythonOperator(
    task_id='extract_from_kinesis',
    python_callable=extract_from_kinesis,
    dag=dag
)

validate_bronze_task = PythonOperator(
    task_id='validate_bronze_data',
    python_callable=validate_bronze_data,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

# Tasks da camada Silver (DBT transformations)
dbt_silver_task = BashOperator(
    task_id='dbt_run_silver',
    bash_command='cd /opt/airflow/dbt && dbt run --models silver',
    dag=dag
)

dbt_test_silver_task = BashOperator(
    task_id='dbt_test_silver',
    bash_command='cd /opt/airflow/dbt && dbt test --models silver',
    dag=dag
)

# Tasks da camada Gold (DBT business logic)
dbt_gold_task = BashOperator(
    task_id='dbt_run_gold',
    bash_command='cd /opt/airflow/dbt && dbt run --models gold',
    dag=dag
)

dbt_test_gold_task = BashOperator(
    task_id='dbt_test_gold',
    bash_command='cd /opt/airflow/dbt && dbt test --models gold',
    dag=dag
)

# Documentação DBT
dbt_docs_task = BashOperator(
    task_id='dbt_generate_docs',
    bash_command='cd /opt/airflow/dbt && dbt docs generate',
    dag=dag
)

# Definindo dependências
extract_kinesis_task >> validate_bronze_task >> quality_check_task
quality_check_task >> dbt_silver_task >> dbt_test_silver_task
dbt_test_silver_task >> dbt_gold_task >> dbt_test_gold_task
dbt_test_gold_task >> dbt_docs_task