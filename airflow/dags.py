from datetime import datetime
from airflow import DAG
import os
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud import bigquery, storage
import requests
import pandas as pd
from a_setup import topics
from b_extract_copy import run 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/subject-screener-402918-c2016ea141c2.json"


my_variables = {
    "period": "1d",
    "subject": topics,
    "monitor": True
}

def save_to_gcs(**context):
    # Carga el DataFrame desde XComs
    df_csv = context['task_instance'].xcom_pull(task_ids='extract_and_load_to_df')
    df = pd.read_csv(df_csv)

    # Guarda el DataFrame en un archivo parquet
    df.to_parquet('data.parquet')

    # Sube el archivo parquet a Google Cloud Storage
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id='my_gcs_connection')
    hook.upload(bucket='subject-screener1', object='data.parquet', filename='/dfs/data.parquet')

def load_to_bq():
    # Crea la tabla en BigQuery a partir del archivo parquet en Google Cloud Storage
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    uri = "gs://subject-screener1/dfs/"
    dataset_ref = client.dataset('compiled_data')
    table_ref = dataset_ref.table('news_by_subject')
    
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}

dag = DAG('api_to_bq', 
          description='Extract data from API, load into pandas, save to GCS, and load to BQ',
          schedule_interval='0 12 * * *',
          default_args=default_args,
          catchup=False)

task1 = PythonOperator(task_id='extract_and_load_to_df', 
                       python_callable=run, 
                       provide_context=True,
                       dag=dag)


task2 = PythonOperator(task_id='save_to_gcs', 
                       python_callable=save_to_gcs, 
                       provide_context=True,
                       dag=dag)

task3 = PythonOperator(task_id='load_to_bq', 
                       python_callable=load_to_bq, 
                       dag=dag)

task1 >> task2 >> task3