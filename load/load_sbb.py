#config = dotenv_values(".env")
#RDS_HOST = config['RDS_HOST']
#RDS_DBNAME = config['RDS_DBNAME']
#RDS_USERNAME = config['RDS_USERNAME']
#RDS_PASSWORD = config['RDS_PASSWORD']

# https://towardsdatascience.com/sqlalchemy-python-tutorial-79a577141a91

import sqlalchemy as db
import pandas as pd
#import pg8000

#def conn_db():
engine = db.create_engine('postgresql+pg8000://municipal:12345678@municipal.czlcabwptkim.us-east-1.rds.amazonaws.com:5432')

connection = engine.connect()
metadata = db.MetaData()
tbl_test = db.Table('test', metadata, autoload=True, autoload_with=engine)
print(repr(metadata.tables['test']))

#open questions: 6'000 anfragen pro tag über api möglich, aber dürfte auch 1x das ganze datenset als dataset runterladen...
# 6k/tag wäre "dynamisch", abermühsam halt...