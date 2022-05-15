import logging
from sqlalchemy.sql import text
from municipality_quality.database_management import dbconnector as db_mgmt

def upload_to_db_dwh(airflow_connection_db: str, job_id: int, proc_name: str, db_schema: str) -> None:

    conn = db_mgmt.DBConnector(airflow_connection_db)
    engine = conn.connect_datalake()
    connection = engine.connect()
    logging.info("Connection to db established")

    parameter1 = job_id
    parameter2 = db_schema + "." + proc_name

    try:
        connection.execute(text("CALL :p2 (:p1)"),p1=parameter1, p2=parameter2)
        connection.execute("commit")
    except:
        logging.info('Could not execute dwh procedure. Please check db.')
    finally:
        connection.close()
