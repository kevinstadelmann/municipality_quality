import datetime as dt
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from load import load_to_s3

def main_function(url: str, encoding: str, s3_bucket_name: str, path: str):
    # Dummy function
    load_to_s3.upload_to_s3(url,encoding,s3_bucket_name,path)



with DAG(
        dag_id='refresh_datalake',
        schedule_interval='@daily',
        start_date=datetime(2022, 3, 1),
        catchup=False,
        tags=["dataocean"]

) as dag:
    # Upload the file
    refresh_datalake = PythonOperator(
        task_id='refresh_datalake',
        python_callable=main_function,
        op_kwargs={
            'url': 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv',
            'encoding': 'ISO-8859-1',
            's3_bucket_name': 'dataocean-datalake',
            'path': '02_Wather_Station/weather_station_main_{}'.format(dt.datetime.now())\
                        .replace(".", "_")\
                        .replace(":", "_")\
                        .replace(" ", "_")\
                        + '.csv'
        }
    )
