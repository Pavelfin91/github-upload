import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import auth


res = requests.get("https://randomuser.me/api/?results=4500")
if res.ok:
    dataset = pd.json_normalize(json.loads(res.text), record_path=['results'])


engine = create_engine('mysql+pymysql://'+str(auth.user)+':'+str(auth.pw)+'@'+str(auth.host)+':'
+str(auth.port)+'/'+str(auth.database))
#                        .format(host=auth.host, db=auth.database, user=auth.user, pw=auth.password))
conn = engine.connect()

male_dataset = dataset.query("gender == 'male'")
male_dataset.to_sql(con=conn, name='PAVEL_test_male', if_exists='replace', index=False)

female_dataset = dataset.query("gender == 'female'")
female_dataset.to_sql(con=conn, name='PAVEL_test_male', if_exists='replace', index=False)

# max_age_df = dataset['dob.age']
# print(max_age_df.max())
# max_age = int(round(max_age_df.max())/10)

for i in range(10):
    age_subset = dataset[((i+1) * 10 <= dataset['dob.age']) & (dataset['dob.age'] < (i + 2) * 10)]
    age_subset.to_sql(con=conn, name='PAVEL_test_'+str(i+1), if_exists='replace', index=False)

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


twenty_join_five_sql = """\
                           (SELECT *
                           FROM PAVEL_test_20)
                           UNION
                           (SELECT *
                           FROM PAVEL_test_5)"""

twenty_join_five = pd.read_sql_query(twenty_join_five_sql, engine)
twenty_join_five.to_json(path_or_buf="C:/Users/pavel/PycharmProjects/Wix_Bi/first.json")


twenty_join_two_sql = """\
                           (SELECT *
                           FROM PAVEL_test_20)
                           UNION ALL
                           (SELECT *
                           FROM PAVEL_test_5)"""

twenty_join_two = pd.read_sql_query(twenty_join_two_sql, engine)
twenty_join_two.to_json(path_or_buf="C:/Users/pavel/PycharmProjects/Wix_Bi/second.json")


# mycursor = mydb.cursor()
# mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")