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




index_cols = ['LATITUDE','LONGITUDE','DATE']

field_cols = [
'HourlyDewPointTemperature',
'HourlyDryBulbTemperature',
'HourlyPrecipitation',
'HourlyPresentWeatherType',
'HourlyPressureChange',
'HourlyPressureTendency',
'HourlyRelativeHumidity',
'HourlySkyConditions',
'HourlySeaLevelPressure',
'HourlyStationPressure',
'HourlyVisibility',
'HourlyWetBulbTemperature',
'HourlyWindGustSpeed',
'HourlyWindSpeed']

def filesensor():
    time.sleep(5) # 5 sec timeout
    files = os.listdir('output')
    return  'data.zip' in files

def list_files(start_path):
    all_files = []
    for root, dirs, files in os.walk(start_path):
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)
    return all_files

def find_all_file_paths(folder_path):
    # Collect the file paths
    years = os.listdir()
    folder_path = 'extracted_data/data/'
    files = list_files(folder_path)
    files = [file for file in files if file.endswith('.csv')]
    with open('extracted_data/index.json','w') as f:
        json.dump(files,f) #
    return files


def parse_date_month(date):
    try:
        return int(date.split('-')[1])
    except:
        return np.nan
    

def parse_date_year(date):
    try:
        return int(date.split('-')[0])
    except:
        return np.nan
    
class ReadAndCleanCSVs(beam.DoFn):
    def process(self, file_path, **kwargs):
        field_cols = kwargs['field_cols']
        # Read CSV
        df = pd.read_csv(file_path,low_memory=False)
        # Assuming 'DATE', 'LATITUDE', and 'LONGITUDE' are always present

        df['MONTH'] = df['DATE'].apply(parse_date_month)
        df['YEAR'] = df['DATE'].apply(parse_date_year)
        df.drop(columns=['DATE'], inplace=True)
        
        # Clean and convert fields
        for col in field_cols:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[^0-9.+-]', ''), errors='coerce')

        index_cols = ['LATITUDE','LONGITUDE','MONTH', 'YEAR']
        df = df[index_cols + field_cols] 

        ret =  [(tuple(x[0:4]), tuple(x[4:])) for x in df.itertuples(index=False, name=None)]
        return ret

class ComputeMean(beam.DoFn):
    def process(self, element, **kwargs):
        key, values = element
        # Convert values to numeric, using np.nan for any conversion failures
        numeric_values = []
        for value in values:
            # Assuming 'value' is a tuple of field column values
            numeric_row = []
            for item in value:
                try:
                    numeric_row.append(float(item))
                except ValueError:
                    numeric_row.append(np.nan)  # Use np.nan for non-numeric values
            numeric_values.append(numeric_row)
        
        # Now, numeric_values is guaranteed to contain only floats and np.nan
        # Calculate mean while ignoring np.nan values
        if numeric_values:  # Check if numeric_values is not empty
            mean_values = np.nanmean(numeric_values, axis=0)
            # Ensure mean_values is a list or tuple before concatenation
            mean_list = mean_values.tolist() if hasattr(mean_values, 'tolist') else mean_values
            return [(key, tuple(mean_list))]
        else:
            # Return no value if numeric_values is empty
            return []

def format_csv(element):
    key, mean_values = element
    return ','.join(map(str, key + tuple(mean_values)))

def run_processing_pipeline(field_cols, output_path):
    with open('extracted_data/index.json') as f:
        files = json.load(f)
    with beam.Pipeline() as pipeline:
        records = (
            pipeline
            | 'Create File Paths' >> beam.Create(files)
            | 'Read and Clean CSVs' >> beam.ParDo(ReadAndCleanCSVs(), field_cols=field_cols)
        )

        grouped_records = (
            records
            | 'Group by Key' >> beam.GroupByKey()
            | 'Compute Mean by Key' >> beam.ParDo(ComputeMean())
        )

        header = ','.join(['LATITUDE','LONGITUDE','MONTH', 'YEAR']+field_cols)
        
        output = (
            grouped_records
            | 'Format CSV' >> beam.Map(format_csv)
            | 'Write to CSV' >> beam.io.WriteToText(output_path, file_name_suffix='.csv', shard_name_template='',
                                                    header=header)
        )

def make_plots_by_field(gdf, field):
    grouped_data = gdf.groupby(['LATITUDE','LONGITUDE','MONTH', 'YEAR'])[field].mean().reset_index()

    # Create a folder for saving visualizations for each field
    output_folder_field = os.path.join('plots', field)
    os.makedirs(output_folder_field, exist_ok=True)

    for index, group in grouped_data.groupby(['MONTH', 'YEAR']):
        month, year = index
        month_year = f"{month:02d}_{year}"

        fig, ax = plt.subplots(1, 1, figsize=(12, 10))
        group.plot(ax=ax, kind='scatter', x='LONGITUDE', y='LATITUDE', c=field, cmap='YlOrRd', legend=True,
                #    legend_kwargs={'label': field},
                     s=50, edgecolor='black', linewidth=0.5)
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        world.plot(ax=ax, color='lightgray', edgecolor='black')
        ax.set_title(f'Heatmap: {field} - {month_year}')
        ax.set_axis_off()

        output_folder_year = os.path.join(output_folder_field, str(year))
        os.makedirs(output_folder_year, exist_ok=True)

        # Save the plot in the specified folder structure
        output_file_path = os.path.join(output_folder_year, f"{month:02d}.png")

        plt.savefig(output_file_path, bbox_inches='tight')
        plt.close()

def visualisation_pipeline(heatmap_fields):
    df = pd.read_csv('output/processed.csv')

    # Make sure 'LATITUDE' and 'LONGITUDE' columns are in numeric format
    df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')

    # Create a GeoDataFrame from the DataFrame
    geometry = [Point(xy) for xy in zip(df['LONGITUDE'], df['LATITUDE'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    # Group by MONTH and YEAR and create plots
    for field in heatmap_fields:
        make_plots_by_field(gdf, field)
    