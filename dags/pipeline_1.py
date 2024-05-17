import os
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

def scrape(ds, year, **kwargs):
    print('cwd:',os.getcwd())
    logging.info(f'Current working directory: {os.getcwd()}')
    base_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}'
    os.makedirs(f'/Users/saraths17/airflow/climatedata/{year}', exist_ok=True) 
    url = base_url.format(year=year)
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    table = soup.find('table')
    anchors = table.find_all('a')
    anchors = [a for a in anchors if 'csv' in a.text]
    for anchor in anchors:
        file = anchor.text
        file_url = f'{url}/{file}'
        res = requests.get(file_url)
        csv = res.text
        with open(f'/Users/saraths17/airflow/climatedata/{year}/{file}', 'w') as f:  
            f.write(csv)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'task_1_dag',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule_interval as needed
)

archive_task = BashOperator(
    task_id='archive_data',
    bash_command='zip -r /Users/saraths17/airflow/climatedata/data.zip /opt/airflow/data && mkdir -p /Users/saraths17/airflow/climatedata && mv /Users/saraths17/airflow/climatedata/data.zip /Users/saraths17/airflow/climatedata/',
    dag=dag,
)



gather_task = DummyOperator(
    task_id='gather_scrape_tasks',
    dag=dag,
)

for year in range(2000, 2024):
    task_id = f'scrape_data_{year}'
    scrape_task = PythonOperator(
        task_id=task_id,
        python_callable=scrape,
        op_kwargs={'year':year},
        provide_context=True,
        dag=dag,
    )
    scrape_task >> gather_task  # Set dependency

gather_task >> archive_task  # Ensure archive_task starts after all scrape_tasks

