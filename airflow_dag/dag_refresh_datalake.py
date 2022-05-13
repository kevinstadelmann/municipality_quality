import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from municipality_quality.extraction import extract_from_source
from municipality_quality.load import load_to_s3
from municipality_quality.load import load_to_db
from municipality_quality.database_management import dblogger as log
from municipality_quality.database_management import dbconnector as dbconn

def main_function(airflow_connection_db, table_job_metadata):
    db_logger = log.DBLogger(airflow_connection_db)

    # Get job meta information
    db_conn = dbconn.DBConnector(airflow_connection_db)
    db_conn.engine = db_conn.connect_datalake()
    query = 'SELECT * FROM ' + table_job_metadata + ' order by job_id;'
    df_jobs = pd.read_sql_query(query, db_conn.engine)

    for index, row in df_jobs.iterrows():
        db_logger.write_log(row['job_id'], 0, 'Job start')
        try:
            db_logger.write_log(row['job_id'], 0, 'Download file start')
            data_stream = extract_from_source.download_file(row['file_url'], row['file_encoding'], row['file_type'], row['file_tab'], row['file_skiprows'])
            db_logger.write_log(row['job_id'], 0, 'Download file end')
        except:
            db_logger.write_log(row['job_id'], 99, 'Download file failed')
        try:
            db_logger.write_log(row['job_id'], 0, 'Upload to s3 start')
            load_to_s3.upload_to_s3(data_stream, row['airflow_connection_s3'], row['aws_s3_bucket_name'], row['aws_s3_bucket_path'])
            db_logger.write_log(row['job_id'], 0, 'Upload to s3 end')
        except:
            db_logger.write_log(row['job_id'], 99, 'Upload to s3 failed')
        try:
            db_logger.write_log(row['job_id'], 0, 'Upload to db start')
            load_to_db.upload_to_db(data_stream, row['file_sep'], row['airflow_connection_db'], row['table_name'], row['db_schema'], row['file_type'])
            db_logger.write_log(row['job_id'], 0, 'Upload to db end')
        except:
            db_logger.write_log(row['job_id'], 99, 'Upload to db failed')
        db_logger.write_log(row['job_id'], 0, 'Job end')


def insert_log(airflow_connection_db, log_text):
    db_logger = log.DBLogger(airflow_connection_db)
    db_logger.write_log(0,0,log_text)


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
            'log_text': 'Start DAG refresh_datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean'
        } )

end_dag = PythonOperator(
        task_id='99_end_dag',
        dag=dag,
        python_callable=insert_log,
        op_kwargs={
            'log_text': 'End DAG refresh_datalake',
            'airflow_connection_db':'aws_rds_postgres_dataocean'
        } )

load_data = PythonOperator(
        task_id='01_load_data',
        dag=dag,
        python_callable=main_function,
        op_kwargs={
            'table_job_metadata': 'job.v_job',
            'airflow_connection_db':'aws_rds_postgres_dataocean'
        } )

start_dag >> load_data >> end_dag