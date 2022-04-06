import logging
import sqlalchemy as db
import pandas as pd
from io import StringIO
# from airflow.hooks.postgres_hook import PostgresHook

from municipality_quality.database_management import database_management as db_mgmt

def upload_to_db(data_stream, sep: str, airflow_connection_db: str, table_name: str) -> None:
    # Function to upload files to db
    logging.info("Upload to db start")
    df = pd.read_csv(StringIO(data_stream), sep=sep)

    metadata, engine = db_mgmt.connect_datalake(airflow_connection_db)
    logging.info("Connection to db established")

    df.to_sql(table_name, con=engine, if_exists='replace')

    logging.info("Upload to db end")