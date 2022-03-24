# https://docs.sqlalchemy.org/en/14/core/metadata.html#creating-and-dropping-database-tables
from sqlalchemy import *
import pandas as pd
#import pg8000
# import logging -> create logs, really professional :)
import os
import dotenv as dot
import database_management as db_mgmt

# start connection to data lake
metadata, engine = db_mgmt.connect_datalake()

#print(metadata)

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

weather = Table('weather_data', metadata,
    Column('plz4', Integer, primary_key=True),
    Column('plzz', String(60), nullable=False, key='name'),
    Column('plznamk', Integer),
    Column('ktkz', String(2)),
    Column('gdenr', String(4)),
    Column('gdenamk', String(50))
)

# checkfirst -> if tables exists or not
municipal.create(engine, checkfirst=True)
weather.create(engine, checkfirst=True)