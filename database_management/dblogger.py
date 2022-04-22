import logging
from sqlalchemy.sql import text
from municipality_quality.database_management import dbconnector as db_mgmt

class DBLogger:
    def __init__(self, airflow_connection_db: str):
        conn = db_mgmt.DBConnector(airflow_connection_db)
        self.engine = conn.connect_datalake()

    def write_log(self, job_id: int, error_level: int, log_text: str):
        try:
            connection = self.engine.connect()
        except:
            logging.info('Connection could not be established')
            return None

        parameter1 = job_id
        parameter2 = error_level
        parameter3 = log_text
        logging.info(connection)
        try:
            connection.execute(text("CALL job.p_log (:p1, :p2, :p3)"),p1=parameter1, p2=parameter2, p3=parameter3)
            connection.execute("commit")
        except:
            logging.info('Could not write into db log. Please check, if table log.t_log and procedure public.p_log exist.')
        finally:
            connection.close()




