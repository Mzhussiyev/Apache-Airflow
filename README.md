# Apache-Airflow


# В Amazon Redshift с недавних пор появились возможность выгрузить данные сразу в бинарный колоночный формат Parquet

UNLOAD (
'
SELECT
    created_date,
    campaign,
    campaign_source,
    placement,
    installs,
    payers,
    revenue1,
    revenue2
FROM big_data_table
')
TO 's3://big-data-bucket/unload/'
CREDENTIALS 'aws_access_key_id=<key>;aws_secret_access_key=<secret>'
FORMAT PARQUET
PARTITION BY created_date;
  
  
  
# Airflow with TaskFlow API
  
import os
import datetime as dt

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    dag_id='titanic_dag',
    start_date=dt.datetime(2021, 3, 1),
    schedule_interval='@once'
) as dag:

    check_if_file_exists = SimpleHttpOperator(
        method='HEAD',
        task_id='check_file_existence',
        http_conn_id='web_stanford_http_id',
        endpoint='/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv',
    )

    @task
    def download_titanic_dataset():
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        response = requests.get(url, stream=True)
        response.raise_for_status()
        filepath = os.path.join(os.path.expanduser('~'), 'titanic.csv')
        with open(filepath, 'w', encoding='utf-8') as f:
            for chunk in response.iter_lines():
                f.write('{}\\n'.format(chunk.decode('utf-8')))
        return filepath

    @task
    def get_number_of_lines(file_path):
        lines = 0
        with open(file_path) as f:
            for line in f:
                if line:
                    lines += 1
        return lines

    file_path = download_titanic_dataset()
    number_of_lines = get_number_of_lines(file_path)

    check_if_file_exists >> file_path

    # check_if_file_exists >> file_path >> number_of_lines  # так тоже можно
  
  
# Airflow operator  
import os
import datetime as dt

import requests
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 2, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

FILENAME = os.path.join(os.path.expanduser('~'), 'titanic.csv')

def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(FILENAME, 'w', encoding='utf-8') as f:
        for chunk in response.iter_lines():
            f.write('{}\\n'.format(chunk.decode('utf-8')))

def pivot_dataset():
    titanic_df = pd.read_csv(FILENAME)
    pvt = titanic_df.pivot_table(
        index=['Sex'], columns=['Pclass'], values='Name', aggfunc='count'
    )
    df = pvt.reset_index()
    df.to_csv(os.path.join(os.path.expanduser('~'), 'titanic_pivot.csv'))

with DAG(dag_id='titanic_pivot', default_args=args, schedule_interval=None) as dag:
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
        dag=dag
    )
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset,
        dag=dag
    )
    create_titanic_dataset >> pivot_titanic_dataset
