# https://www.sqlshack.com/introduction-to-sqlalchemy-in-pandas-dataframe/

import pandas as pd
from database_management.database_management import *
from sqlalchemy import *


df_municipal = pd.read_excel("../data/do-t-09.02-gwr-37_PLZ_to_BFS-Gemeindenr.xlsx", sheet_name='PLZ4')

metadata, engine = connect_datalake()

#table = sqlalchemy.Table('municipal', metadata, autoload=True)
# Inserting records via SQLAlchemy `Table` objects
stmt = insert('public.municipal').values(
        plz4=1000,
        plzz='testgemeinde',
        plznamk=123,
        ktkz='LU',
        gdenr='asdd',
        gdenamk='asd'
    )

print(stmt)

# stmt = test("SELECT * FORM public.municipal")
#result = connection.execute,

with engine.connect() as conn:
    result = conn.execute(stmt)
    conn.commit()










#table = sqlalchemy.Table('municipal', metadata, autoload=True)

#df_municipal.to_sql('municipal1', engine, if_exists='replace')




#df_municipal.to_sql('municipal1', engine)

# Insert the dataframe into the database in one bulk
#conn.execute(table.insert(), df_municipal, autoload=True)

