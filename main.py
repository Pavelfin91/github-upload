import json

import requests
import mysql.connector
import pandas as pd

res = requests.get("https://randomuser.me/api/?results=4500")
if res.ok:
    df = pd.json_normalize(json.loads(res.text), record_path=['results'])
    mydb = mysql.connector.connect(user='interview_user',
                                   password="aviv_2021_07_06_!!@@QQ",
                                   host='104.197.7.195',
                                   database='interview')


# mycursor = mydb.cursor()
# mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")
