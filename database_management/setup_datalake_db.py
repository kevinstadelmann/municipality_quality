# https://docs.sqlalchemy.org/en/14/core/metadata.html#creating-and-dropping-database-tables
from sqlalchemy import *
import pandas as pd
#import pg8000
# import logging -> create logs, really professional :)
import os
import dotenv as dot


# load environment variables
dot.load_dotenv()
str_host = os.getenv('RDS_HOST')
str_user = os.getenv('RDS_USERNAME')
str_pw = os.getenv('RDS_PASSWORD')

engine = create_engine('postgresql+pg8000://' + str_user + ':' + str_pw + '@' + str_host)

connection = engine.connect()
metadata = MetaData()

# municipal table
# source: federal office of statistics
municipal = Table('municipal', metadata,
    Column('plz4', Integer, primary_key=True),
    Column('plzz', String(60), nullable=False, key='name'),
    Column('plznamk', Integer),
    Column('ktkz', String(2)),
    Column('gdenr', String(4)),
    Column('gdenamk', String(50))
)

# checkfirst -> if tables exists or not
municipal.create(engine, checkfirst=True)