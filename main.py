import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import auth

# First Stage

# Sending a GET request to the mentioned API retrieving 4500 records
# then, parse the string representation of JSON file to rows of tabular form
res = requests.get("https://randomuser.me/api/?results=4500")
if res.ok:
    main_dataset = pd.json_normalize(json.loads(res.text), record_path=['results'])

# Establishing connection to the database
engine = create_engine('mysql+pymysql://' + str(auth.user) + ':' + str(auth.pw) + '@' + str(auth.host) + ':'
                       + str(auth.port) + '/' + str(auth.database))

# connect to MYSQLDB
conn = engine.connect()

# Second Stage

# creating new datasets via queries and inserting them to DB
male_dataset = main_dataset.query("gender == 'male'")
female_dataset = main_dataset.query("gender == 'female'")

male_dataset.to_sql(con=conn, name='PAVEL_test_male', if_exists='replace', index=False)
female_dataset.to_sql(con=conn, name='PAVEL_test_female', if_exists='replace', index=False)

# Third Stage

# create the dob.age-subsets and insert the resulting dataframes to DB
for i in range(10):
    age_subset = main_dataset[((i + 1) * 10 <= main_dataset['dob.age']) & (main_dataset['dob.age'] < (i + 2) * 10)]
    age_subset.to_sql(con=conn, name='PAVEL_test_' + str(i + 1), if_exists='replace', index=False)

# Fourth Stage

last_reg_sql = """\
               (SELECT *
               FROM PAVEL_test_male
               ORDER BY 'registered.date' DESC
               LIMIT 20)
               UNION
               (SELECT *
               FROM PAVEL_test_female
               ORDER BY 'registered.date' DESC
               LIMIT 20)"""

# querying the DB
last_reg = pd.read_sql_query(last_reg_sql, engine)
# inserting DF to DB
last_reg.to_sql(con=conn, name='PAVEL_test_20', if_exists='replace', index=False)

# Fifth Stage

twenty_union_five_sql = """\
                           (SELECT *
                           FROM PAVEL_test_20)
                           UNION
                           (SELECT *
                           FROM PAVEL_test_5)"""

# querying the DB and creating JSON from dataframe
twenty_union_five = pd.read_sql_query(twenty_union_five_sql, engine)
twenty_union_five.to_json(path_or_buf="C:/Users/pavel/PycharmProjects/Wix_Bi/first.json")

# Sixth Stage

twenty_union_two_sql = """\
                           (SELECT *
                           FROM PAVEL_test_20)
                           UNION ALL
                           (SELECT *
                           FROM PAVEL_test_2)"""

# querying the DB and creating JSON from dataframe
twenty_union_two = pd.read_sql_query(twenty_union_two_sql, engine)
twenty_union_two.to_json(path_or_buf="C:/Users/pavel/PycharmProjects/Wix_Bi/second.json")

conn.close()