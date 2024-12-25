from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from etl.extract_data_api import get_data
from etl.tranfrom_data import tranform_data_df
from etl.load_data import load


city_name = "Bangkok"
My_Api_Key = "c743fa590308f54f2a59f8f121d6b573"
api_url= f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={My_Api_Key}"

def extract_task_callable(**context):
    data = get_data(api_url)
    return data

def transform_task_callable(**context):
    data = context['ti'].xcom_pull(task_ids='extract_weather_data')
    transformed_data = tranform_data_df(data)
    return transformed_data

def load_task_callable(**context):
    transformed_data = context['ti'].xcom_pull(task_ids='transform_weather_data')
    load(transformed_data)

dag = DAG(
    'weather_dag',
    description='DAG for Weather Data Pipeline',
    schedule_interval='@hourly',
    start_date=datetime(2024, 12, 23),
    catchup=False,
    
)

extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_task_callable,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_task_callable,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_weather_data_to_database',
    python_callable=load_task_callable,
    dag=dag,
)

extract_task >> transform_task >> load_task