import logging
import re
import pandas as pd
from io import StringIO
from municipality_quality.database_management import dbconnector as db_mgmt

def upload_to_db(data_stream, sep: str, airflow_connection_db: str, table_name: str, db_schema: str, file_type: str) -> None:
    # Function to upload files to db
    if file_type == 'csv':
        df = pd.read_csv(StringIO(data_stream), sep=sep)
    elif file_type == 'json':
        df = pd.read_json(StringIO(data_stream))
    elif file_type == 'xlsx':
        df = pd.read_csv(StringIO(data_stream))
    else:
        logging.info("Unknown file type")

    conn = db_mgmt.DBConnector(airflow_connection_db)
    engine = conn.connect_datalake()
    logging.info("Connection to db established")

    ws = re.compile("\s+")
    # lower the case, strip leading and trailing white space,
    # and substitute the whitespace between words with underscore
    df.columns = [ws.sub("_", i.lower().strip()) for i in df.columns]
    df.to_sql(table_name, con=engine, schema=db_schema, if_exists='replace', chunksize=500, method='multi', index=False)
