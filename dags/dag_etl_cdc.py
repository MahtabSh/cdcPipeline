# Import necessary modules from Airflow and other libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import datetime, timedelta
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
import os
import pymysql

# Import custom function for data transformation
from scripts.parse_func import apply_tansformation


def extract_transform_load_data():
    """
    Extracts data from MySQL binary logs, transforms it, and loads it into a target MySQL database.
    """
    # Transform the extracted data as needed
    # target_mysql_hook = MySqlHook(mysql_conn_id='target_mysql_conn')
    # target_connection = target_mysql_hook.get_conn()
    # target_cursor = target_connection.cursor()

    # Connect to the target MySQL database
    target_mysql_settings = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': 'dwhPassWprd', 'db': 'dwh'}
    target_connection = pymysql.connect(**target_mysql_settings)
    target_cursor = target_connection.cursor()

    # Connect to the MySQL binary log stream
    mysql_settings = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'passwd': 'dwhPassWprd'
    }

    stream = BinLogStreamReader(connection_settings=mysql_settings, server_id=1)
    # Iterate over events in the binary log stream
    for binlogevent in stream:
        # if start_time < binlogevent.timestamp <= end_time:
        if isinstance(binlogevent, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            # Apply transformation to the event and load it into the target MySQL database
            apply_tansformation(binlogevent, target_cursor, binlogevent.table)
        
        # Commit the changes
        target_connection.commit()

    # Close the binary log stream
    stream.close()


# Define a DAG (Directed Acyclic Graph) for the workflow
with DAG('mysql_cdc_dag', start_date=datetime(2024, 4, 30), schedule_interval=None, catchup=False) as dag:
    # Define a task to execute the data extraction, transformation, and loading process
    transform_load_task = PythonOperator(
        task_id='transform_data',
        python_callable=extract_transform_load_data,
    )

    extract_transform_load_data 
