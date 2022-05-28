import logging
from sqlalchemy import exc
from municipality_quality.database_management import dbconnector as db_mgmt

def upload_to_db_dwh(airflow_connection_db: str, job_id: str, proc_name: str, db_schema: str):
    conn = db_mgmt.DBConnector(airflow_connection_db)
    engine = conn.connect_datalake()
    connection = engine.connect()
    logging.info('Connection to db established')

    parameter1 = str(job_id)
    parameter2 = db_schema + "." + proc_name
    statement = 'CALL ' + parameter2 + ' (' + parameter1 + ')'

    try:
        return_message = ['0', 'Load to dwh end']
        connection.execute(statement)
        connection.execute("commit")
        connection.close()
    except exc.SQLAlchemyError as err:
        return_message = [99, f'Failed to execute {statement}: {str(err.orig)}']
    finally:
        connection.close()

    return return_message
