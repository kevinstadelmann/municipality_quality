import logging
import datetime as dt
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from municipality_quality.extraction import extract_from_source
from municipality_quality.load import load_to_s3
from municipality_quality.load import load_to_db

def main_function(url: str, encoding: str, sep: str, airflow_connection_s3, bucket_name_s3: str, path: str, airflow_connection_db: str, table_name: str):
    data_stream = extract_from_source.download_file(url, encoding)
    load_to_s3.upload_to_s3(data_stream, airflow_connection_s3,bucket_name_s3, path)
    load_to_db.upload_to_db(data_stream, sep, airflow_connection_db, table_name)

def insert_log(log_text: str):
    logging.info(log_text)

dag = DAG(
        dag_id= 'refresh_datalake',
        schedule_interval='@daily',
        start_date=datetime(2022, 3, 1),
        catchup=False,
        tags=["dataocean"])

start_dag = PythonOperator(
        task_id='00_start_dag',
        dag=dag,
        python_callable=insert_log,
        op_kwargs={
            'log_text': 'Start DAG '
        } )

end_dag = PythonOperator(
        task_id='99_end_dag',
        dag=dag,
        python_callable=insert_log,
        op_kwargs={
            'log_text': 'End DAG'
        } )

load_sbb_stop = PythonOperator(
        task_id='01_load_sbb_stop',
        dag=dag,
        python_callable=main_function,
        op_kwargs={
            'url': 'https://data.sbb.ch/api/v2/catalog/datasets/dienststellen-gemass-opentransportdataswiss/exports/json?limit=1&offset=0&timezone=UTC&apikey=8a89815df44a86552121399631d28f4472d3a150a9f1d6686ceb5c09',
            'encoding': 'UTF-8',
            'sep': ',',
            'airflow_connection_s3':'aws_s3_dataocean-datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean',
            'table_name': 't_sbb_stop',
            'bucket_name_s3': 'dataocean-datalake',
            'path': '01_SBB_stop/sbb_stop_{}'.format(dt.datetime.now())\
                        .replace(".", "_")\
                        .replace(":", "_")\
                        .replace(" ", "_")\
                        + '.csv'
        } )

load_weather_station = PythonOperator(
        task_id='02_load_weather_station',
        dag=dag,
        python_callable=main_function,
        op_kwargs={
            'url': 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv',
            'encoding': 'ISO-8859-1',
            'sep': ';',
            'airflow_connection_s3':'aws_s3_dataocean-datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean',
            'table_name': 't_weather_station',
            'bucket_name_s3': 'dataocean-datalake',
            'path': '02_Weather_Station/weather_station_main_{}'.format(dt.datetime.now())\
                        .replace(".", "_")\
                        .replace(":", "_")\
                        .replace(" ", "_")\
                        + '.csv'
        } )

load_weather_statistic = PythonOperator(
        task_id='03_load_weather_statistic',
        dag=dag,
        python_callable=main_function,
        op_kwargs={
            'url': 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv',
            'encoding': 'ISO-8859-1',
            'sep': ';',
            'airflow_connection_s3':'aws_s3_dataocean-datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean',
            'table_name': 't_weather_statistic',
            'bucket_name_s3': 'dataocean-datalake',
            'path': '03_Weather_Statistic/weather_statistic_{}'.format(dt.datetime.now())\
                        .replace(".", "_")\
                        .replace(":", "_")\
                        .replace(" ", "_")\
                        + '.csv'
        }
    )

load_municipality_zipcode = PythonOperator(
        task_id='04_load_municipality_zipcode',
        dag=dag,
        python_callable=main_function,
        op_kwargs={
            'url': 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv',
            'encoding': 'ISO-8859-1',
            'sep': ';',
            'airflow_connection_s3':'aws_s3_dataocean-datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean',
            'table_name': 't_municipality_zipcode',
            'bucket_name_s3': 'dataocean-datalake',
            'path': '04_Municipality_Zipcode/municipality_zipcode_{}'.format(dt.datetime.now())\
                        .replace(".", "_")\
                        .replace(":", "_")\
                        .replace(" ", "_")\
                        + '.csv'
        } )

load_municipality_statistic = PythonOperator(
        task_id='05_load_municipality_statistic',
        dag=dag,
        python_callable=main_function,
        op_kwargs={
            'url': 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv',
            'encoding': 'ISO-8859-1',
            'sep': ';',
            'airflow_connection_s3':'aws_s3_dataocean-datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean',
            'table_name': 't_municipality_statistic',
            'bucket_name_s3': 'dataocean-datalake',
            'path': '05_Municipality_Statistic/municipality_statistic_{}'.format(dt.datetime.now())\
                        .replace(".", "_")\
                        .replace(":", "_")\
                        .replace(" ", "_")\
                        + '.csv'
        } )

load_municipality_district = PythonOperator(
        task_id='06_load_municipality_district',
        dag=dag,
        python_callable=main_function,
        op_kwargs={
            'url': 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv',
            'encoding': 'ISO-8859-1',
            'sep': ';',
            'airflow_connection_s3':'aws_s3_dataocean-datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean',
            'table_name': 't_municipality_district',
            'bucket_name_s3': 'dataocean-datalake',
            'path': '06_Municipality_District/municipality_district_{}'.format(dt.datetime.now())\
                        .replace(".", "_")\
                        .replace(":", "_")\
                        .replace(" ", "_")\
                        + '.csv'
        } )

load_municipality_location = PythonOperator(
        task_id='07_load_municipality_location',
        dag=dag,
        python_callable=main_function,
        op_kwargs={
            'url': 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv',
            'encoding': 'ISO-8859-1',
            'sep': ';',
            'airflow_connection_s3':'aws_s3_dataocean-datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean',
            'table_name': 't_municipality_location',
            'bucket_name_s3': 'dataocean-datalake',
            'path': '07_Municipality_Location/municipality_location_{}'.format(dt.datetime.now())\
                        .replace(".", "_")\
                        .replace(":", "_")\
                        .replace(" ", "_")\
                        + '.csv'
        } )

start_dag >> load_sbb_stop >> load_weather_station >> load_weather_statistic >> load_municipality_zipcode >> load_municipality_statistic >> load_municipality_district >> load_municipality_location >> end_dag