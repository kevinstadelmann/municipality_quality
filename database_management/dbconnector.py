import sqlalchemy as db
from airflow.hooks.base import BaseHook

class DBConnector:
    def __init__(self, airflow_connection_db: str):
        self.airflow_connection_db = airflow_connection_db

    def connect_datalake(self):
        connection = BaseHook.get_connection(self.airflow_connection_db)
        str_host = connection.host
        str_user = connection.login
        str_pw = connection.password
        str_db = connection.schema
        str_port = str(connection.port)
        engine = db.create_engine('postgresql+pg8000://' + str_user + ':' + str_pw + '@' + str_host + ':' + str_port + '/' + str_db)
        engine.dialect.description_encoding = None
        engine.connect()
        return engine