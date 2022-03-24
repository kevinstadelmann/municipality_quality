import os
import dotenv as dot
from sqlalchemy import *

def connect_datalake():
    # load environment variables
    dot.load_dotenv()
    str_host = os.getenv('RDS_DL_HOST')
    str_user = os.getenv('RDS_DL_USERNAME')
    str_pw = os.getenv('RDS_DL_PASSWORD')

    engine = create_engine('postgresql+pg8000://' + str_user + ':' + str_pw + '@' + str_host)

    conn = engine.connect()
    metadata = MetaData()

    return metadata, engine

def connect_datawarehouse():
    # load environment variables
    dot.load_dotenv()
    str_host = os.getenv('RDS_DWH_HOST')
    str_user = os.getenv('RDS_DWH_USERNAME')
    str_pw = os.getenv('RDS_DWH_PASSWORD')

    engine = create_engine('postgresql+pg8000://' + str_user + ':' + str_pw + '@' + str_host)

    connection = engine.connect()
    metadata = MetaData()

    return metadata, engine