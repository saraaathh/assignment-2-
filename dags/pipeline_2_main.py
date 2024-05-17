import os
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import pandas as pd
import numpy as np
import apache_beam as beam
import re
import warnings
warnings.filterwarnings("ignore")
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import time
import json
from task_2_utils import filesensor, find_all_file_paths, run_processing_pipeline, field_cols, visualisation_pipeline
import zipfile

def unzip_file(zip_path, extract_to):
    # Ensure the path is correct and exists
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        print(f"Extracted {zip_path} to {extract_to}")

status = filesensor()
if not status:
    exit()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'task_2_dag_v1',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule_interval as needed
)

# # Unzip
# unzip_bash = 'unzip output/data.zip -d extracted_data/'
# # run unzip bash command in BashOperator here
# unzip_task = BashOperator(
#     task_id="unzipping_data",
#     bash_command=unzip_bash,
#     dag=dag
# )

unzip_task = PythonOperator(
    task_id='unzip_file_task',
    python_callable=unzip_file,
    op_kwargs={'zip_path': 'output/data.zip', 'extract_to': 'extracted_data/'},
    dag=dag,
)

path_finder_pipeline = PythonOperator(
    task_id='file_path_finding_pipeline',
    python_callable=find_all_file_paths,
    op_kwargs={'folder_path': '/extracted_data/'},
    dag=dag,
)

output_path = 'output/processed'
processing_pipeline = PythonOperator(
    task_id='data_processing_pipeline',
    python_callable=run_processing_pipeline,
    op_kwargs={'field_cols': field_cols, 'output_path': output_path},
    dag=dag,
)

heatmap_fields = ['HourlyDewPointTemperature', 'HourlyDryBulbTemperature', 'HourlyPrecipitation']

visualisation_pipeline_task = PythonOperator(
    task_id='data_visualisation_pipeline',
    python_callable=visualisation_pipeline,
    op_kwargs={'heatmap_fields': heatmap_fields},
    dag=dag,
)

delete_extracted_data_task = BashOperator(
    task_id="deleting_csvs",
    bash_command='rm -r extracted_data/',
    dag=dag
)

# Set up dependencies
unzip_task >> path_finder_pipeline >> processing_pipeline >> visualisation_pipeline_task >> delete_extracted_data_task