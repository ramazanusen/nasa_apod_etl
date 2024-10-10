from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import psycopg2
import json
import os
import logging

logging.basicConfig(level=logging.INFO)

load_dotenv()
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'nasa_space_photo_etl_dag',
    default_args=default_args,
    description='A DAG to load NASA APOD data into PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Function to extract data from the NASA APOD API
def extract_apod_data(**kwargs):
    logging.info('Extracting data from NASA API')
    api_key = os.getenv("NASA_API_KEY")
    url = f'https://api.nasa.gov/planetary/apod?api_key={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        apod_data = response.json()
        kwargs['ti'].xcom_push(key='apod_data', value=apod_data)
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

# Function to transform the data
def transform_data(**kwargs):
    logging.info('Transforming data')
    apod_data = kwargs['ti'].xcom_pull(key='apod_data', task_ids='extract_apod_data')
    transformed_data = {
        'date': apod_data.get('date'),
        'title': apod_data.get('title'),
        'explanation': apod_data.get('explanation'),
        'media_type': apod_data.get('media_type'),
        'url': apod_data.get('url')
    }
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def create_table(**kwargs):
    logging.info('Creating table')
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS apod_data (
        date DATE PRIMARY KEY,
        title VARCHAR(255),
        explanation TEXT,
        media_type VARCHAR(50),
        url TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Define the tasks
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    provide_context=True,
    dag=dag,
)
# Function to load the data into PostgreSQL
def load_data_to_postgres(**kwargs):
    logging.info('Loading data to PostgreSQL')
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO apod_data (date, title, explanation, media_type, url)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
    """
    cursor.execute(insert_query, (
        transformed_data['date'],
        transformed_data['title'],
        transformed_data['explanation'],
        transformed_data['media_type'],
        transformed_data['url'],
    ))
    conn.commit()
    cursor.close()
    conn.close()

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_apod_data',
    python_callable=extract_apod_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)

def verify_loaded_data(**kwargs):
    logging.info('Verifying the loaded data')
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database="nasa_space_image",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM public.apod_data ORDER BY date DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        print(f"Last loaded data: Date={row[0]}, Title={row[1]}, Media Type={row[3]}")
    else:
        print("No data loaded")
    cursor.close()
    conn.close()

verify_data_task = PythonOperator(
    task_id='verify_loaded_data',
    python_callable=verify_loaded_data,
    dag=dag,
)

# Set the task dependencies
extract_task >> transform_task >> load_task >> verify_data_task
