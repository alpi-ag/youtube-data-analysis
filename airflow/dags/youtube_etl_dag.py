from airflow import DAG
from airflow.operators.python import PythonOperator
from extract_data_from_api import extract_data_from_api
from transform_data import transform_data
from datetime import timedelta,datetime
import os 


default_args = {'owner': 'airflow',
                'depend_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2025, 4, 18, 0, 0, 0)
                }

dag = DAG(
    'youtube_etl_dag',
    default_args=default_args,
    description='extract trending youtube videos',
    schedule=timedelta(days=1),
    catchup=False
)

extract_task = PythonOperator(task_id="extract_data_from_youtube_api",
                              python_callable = extract_data_from_api,
                              op_kwargs = {
                                'api_key': os.getenv('YOUTUBE_API_KEY'),
                                'region_codes': ['US', 'GB', 'IN', 'AU', 'NZ'],
                                'category_ids': ['1', '2', '10', '15', '20', '22', '23']}
                                ,dag = dag)


clean_task = PythonOperator(task_id = "clean_extracted_data",
                            python_callable = transform_data,
                            dag = dag)

extract_task >> clean_task


