import os
import sys
import pandas as pd
import json
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from sqlalchemy import text
from sqlalchemy import create_engine

cwd=os.getcwd()
sys.path.append(f'../scripts/')
sys.path.append(f'../database/')
sys.path.append(f'../temp/')
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from extract_raw_data import DataExtractor

data_extractor = DataExtractor()
engine = create_engine('postgresql+psycopg2://airflow:airflow@192.168.1.100:8585/postgres')

VEHICLE_SCHEMA = "vehicle_schema.sql"
TRAJECTORIES_SCHEMA = "trajectory_schema.sql"

default_args = {
    'owner': 'Andenet',
    'depends_on_past': False,
    'email': ['andenet.dev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


def create_table ():
    try:
        with engine.connect() as conn:
            for name in [TRAJECTORIES_SCHEMA,VEHICLE_SCHEMA]:
                
                with open(f'/opt/database/{name}', "r") as file:
                    query = text(file.read())
                    conn.execute(query)
        print("Successfull")
    except Exception as e:
        print("Error creating table",e)
        sys.exit(e)


def insert_to_table(json_stream: str, table_name: str, from_file=False):

    try:
        if not from_file:
            df = pd.read_json(json_stream)
        else:
            with open(f'../temp/{json_stream}','r') as file:
                data=file.readlines()
            dt=data[0]

            df=pd.DataFrame.from_dict(json.loads(dt))
            df.columns=df.columns.str.replace(' ','')

            df.dropna(inplace=True)
        with engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False)

    except Exception as e:
        print(f"error insert to table: {e}")  
        sys.exit(e) 

def extract_data(ti):
    loaded_df_name = data_extractor.extract_data(file_name='20181024_d1_0830_0900.csv',return_json=True)
    trajectory_file_name, vehicle_file_name = loaded_df_name

    ti.xcom_push(key="trajectory",value=trajectory_file_name)
    ti.xcom_push(key="vehicle",value=vehicle_file_name)

def seed_db(ti):
    trajectory_file_name = ti.xcom_pull(key="trajectory",task_ids='extract_raw_data')
    vehicle_file_name = ti.xcom_pull(key="vehicle",task_ids='extract_raw_data')
    insert_to_table(trajectory_file_name, 'trajectories', from_file=True)
    insert_to_table(vehicle_file_name, 'vehicles', from_file=True)


with DAG(
    dag_id='extract_and_load', 
    default_args=default_args, 
    description='extracts raw csv data and loads to the database', 
    start_date=datetime(2022,9,24,3), 
    schedule_interval='@daily', 
    catchup=False
) as dag:

    create_tables = PythonOperator(
        task_id = 'create_table',
        python_callable = create_table 
    )

    read_data = PythonOperator(
        task_id='extract_raw_data',
        python_callable = extract_data
    ) 

    seed_db = PythonOperator(
        task_id='seed_db',
        python_callable = seed_db
    )
    
[create_tables, read_data] >> seed_db 

