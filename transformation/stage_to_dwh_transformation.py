"""
Python Script for all transformation work between stage and dwh tables
"""

# import the modules
import pandas as pd
from sqlalchemy import create_engine

# SQLAlchemy connectable
cnx = create_engine('sqlite:///contacts.db').connect()

# table named 'contacts' will be returned as a dataframe.
df = pd.read_sql_table('contacts', cnx)
print(df)
