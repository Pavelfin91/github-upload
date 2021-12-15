import auth
import json
import requests
import pandas as pd
from sqlalchemy import create_engine

# First Stage
# getting data from website via API and parsing it to dataframe
res = requests.get("https://randomuser.me/api/?results=4500")
if res.ok:
    main_dataset = pd.json_normalize(json.loads(res.text), record_path=['results'])
# Establishing connection to the database
engine = create_engine('mysql+pymysql://' + str(auth.user) + ':' + str(auth.pw) + '@' + str(auth.host) + ':'
                       + str(auth.port) + '/' + str(auth.database))
conn = engine.connect()
# Second Stage
# extracting required data and inserting it to Pavel_test_male,Pavel_test_female tables in DB
male_dataset = main_dataset.query("gender == 'male'")
female_dataset = main_dataset.query("gender == 'female'")
male_dataset.to_sql(con=conn, name='PAVEL_test_male', if_exists='replace', index=False)
female_dataset.to_sql(con=conn, name='PAVEL_test_female', if_exists='replace', index=False)
# Third Stage
# creating 10 subsets and inserting each as a table in DB
for i in range(10):
    age_subset = main_dataset[((i + 1) * 10 <= main_dataset['dob.age']) & (main_dataset['dob.age'] < (i + 2) * 10)]
    age_subset.to_sql(con=conn, name='PAVEL_test_' + str(i + 1), if_exists='replace', index=False)
# Fourth Stage
# extracting required data and inserting it to Pavel_test_20 table in DB
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

last_reg = pd.read_sql_query(last_reg_sql, engine)
last_reg.to_sql(con=conn, name='PAVEL_test_20', if_exists='replace', index=False)
# Fifth Stage
# extracting required data and inserting it to a new JSON file
twenty_union_five_sql = """\
                           (SELECT *
                           FROM PAVEL_test_20)
                           UNION
                           (SELECT *
                           FROM PAVEL_test_5)"""
twenty_union_five = pd.read_sql_query(twenty_union_five_sql, engine)
twenty_union_five.to_json(path_or_buf="C:/Users/pavel/PycharmProjects/Wix_Bi/first.json")
# Sixth Stage
# extracting required data and inserting it to a new JSON file
twenty_union_two_sql = """\
                           (SELECT *
                           FROM PAVEL_test_20)
                           UNION ALL
                           (SELECT *
                           FROM PAVEL_test_2)"""
twenty_union_two = pd.read_sql_query(twenty_union_two_sql, engine)
twenty_union_two.to_json(path_or_buf="C:/Users/pavel/PycharmProjects/Wix_Bi/second.json")
conn.close()
