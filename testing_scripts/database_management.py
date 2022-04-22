import os
import logging
#import dotenv as dot
import sqlalchemy as db
from airflow.hooks.base import BaseHook

def connect_datalake(airflow_connection_db: str):
    # load environment variables
    #dot.load_dotenv()
    #str_host = os.getenv('RDS_DL_HOST')
    #str_user = os.getenv('RDS_DL_USERNAME')
    #str_pw = os.getenv('RDS_DL_PASSWORD')
    connection = BaseHook.get_connection(airflow_connection_db)
    str_host = connection.host
    str_user = connection.login
    str_pw = connection.password
    str_db = connection.schema
    str_port = str(connection.port)

    engine = db.create_engine('postgresql+pg8000://' + str_user + ':' + str_pw + '@' + str_host + ':' + str_port + '/' + str_db)
    logging.info('postgresql+pg8000://' + str_user + ':' + str_pw + '@' + str_host + ':' + str_port + '/' + str_db)
    engine.dialect.description_encoding = None
    conn = engine.connect()
    metadata = db.MetaData()
    return metadata, engine

def connect_datawarehouse():
    # load environment variables
    #dot.load_dotenv()
    str_host = os.getenv('RDS_DWH_HOST')
    str_user = os.getenv('RDS_DWH_USERNAME')
    str_pw = os.getenv('RDS_DWH_PASSWORD')

    engine = create_engine('postgresql+pg8000://' + str_user + ':' + str_pw + '@' + str_host)

    connection = engine.connect()
    metadata = MetaData()

    return metadata, engine