#Importing libraries
from flask import Flask, render_template, request, redirect
from flaskext.mysql import MySQL
import yaml
from werkzeug import secure_filename
import json
import pandas as pd
from openpyxl import Workbook
import time
import os

#Configurations
#Time configurations
time.strftime('%Y-%m-%d %H:%M:%S')
#flask configurations
app = Flask(__name__)
# MySQL configurations
db = yaml.load(open('db.yaml'))
app.config['MYSQL_DATABASE_HOST'] = db['mysql_host']
app.config['MYSQL_DATABASE_USER'] = db['mysql_user']
app.config['MYSQL_DATABASE_PASSWORD'] = db['mysql_paasword']
mysql = MySQL()
mysql.init_app(app)
conn = mysql.connect()

#Clean JSON data
def clean_dict_values(D):
    for key,value in D.items():
        old_key = key
        if ' ' in key or ':' in key:

            key = key.replace(':','')
            key = key.strip()
            D[key] = D.pop(old_key)

        D[key] = D[key][0]

@app.route("/")
def index():
    #Check if database exists or create new database.
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS JSONDB")
    cursor.execute("USE JSONDB")
    #Check if tables exists or create new database.
    cursor.execute("SHOW TABLES")
    results = cursor.fetchall()
    all_tables = [item[0] for item in results]

    if "JSONDATA" not in all_tables:
        cursor.execute("""CREATE TABLE JSONDATA(
                            CHARGES VARCHAR(20),
                            GST VARCHAR(20),
                            `SERVICE CHARGES` VARCHAR(20),
                            `STATEMENT DATE` VARCHAR(20),
                            `STATEMENT NUMBER` VARCHAR(20),
                            `TOTAL AMOUNT` VARCHAR(20))""")

    if "tblLog" not in all_tables:
        cursor.execute("""CREATE TABLE tblLog(
                            json_filename VARCHAR(20),
                            time TIME)""")
    conn.commit()

    return render_template("index.html")

@app.route("/", methods = ['POST'])
def handle():

    if not os.path.isdir("JSON_Files"):
        os.mkdir("JSON_Files")
    json_folder = os.path.join(os.getcwd(),"JSON_Files")
    if request.method == 'POST':
        cursor = conn.cursor()
        f = request.files['file']
        json_file = os.path.join(json_folder,secure_filename(f.filename))
        f.save(json_file)
        start_time = time.time()
        try:
            with open(json_file) as json_data:
                d = json.load(json_data)
        except:
            # print("Please upload JSON File")
            return "Please upload JSON File"
        data = []
        for r in d['headerfields']:

            data.append(d['headerfields'][r])
            clean_dict_values(d['headerfields'][r])

        df = pd.DataFrame(data)
        cursor.execute("USE JSONDB")
        print(df.columns)
        cols = "`,`".join([str(i).upper() for i in df.columns.tolist()])
        for i,row in df.iterrows():
            if i < 2:
                sql = "INSERT INTO JSONDATA(`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
            else:
                break;
        conn.commit()

        resultValue = cursor.execute("SELECT * FROM JSONDATA")
        if resultValue > 0:
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(list(data), columns=columns)

            writer = pd.ExcelWriter('data.xlsx')
            df.to_excel(writer, sheet_name='bar')
            writer.save()
            # userDetails = cursor.fetchall()
        query_time = time.time() - start_time
        cursor.execute("INSERT INTO tblLog(json_filename,time) VALUES (%s, %s)"
                        ,(f.filename,query_time))
        conn.commit()
        return render_template('index.html',userDetails = data)

if __name__ == "__main__":
    app.run(debug = True, port = 8008)
