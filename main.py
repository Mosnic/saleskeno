
import mysql.connector
import random
import requests
import json
from datetime import datetime

global sql_host
global sql_user
global sql_password
global sql_DB
global mydb

sql_host="localhost"
sql_user="emo"
sql_password="01101969gSS$"
sql_DB="saleskeno"

def connect_to_mysql():
    return mysql.connector.connect(
        host=sql_host,
        user=sql_user,
        password=sql_password,
        database=sql_DB
    )
def fetch_outs():
    # fetchdate = datetime.strptime("2023-09-17 9:00:00.000", "%d/%m/%Y")
    current_datetime = datetime.now()
    current_date = current_datetime.date()
    date_str = f"{current_date}"
    s_current_datetime = f"{current_datetime}"
    url = f"https://flex.keeneland.com/misc/GenerateJson.do?actionName=VetOutUpdate&paramNames=date%5E%21%5Ereturn_type&paramValues=" + date_str + "%5E%21%5Eouts"
    #Check if fetch_outs has already been run for todays session
    result = query_mysql("SELECT current_session FROM config")
    current_session = result[0][0]
    if current_session < date_str:
        response = requests.get(url)
        if response.status_code == 200:
            data = json.loads(response.text)
            if isinstance(data, list):
                hip_data = [(record["hip_number"], record["out_date"], record["session_date"]) for record in data]
                sql = "INSERT INTO outs (hip_number, out_date, session_date) VALUES (%s, %s, %s)"
                write_to_mysql(sql,hip_data)
            else:
                # Handle case if data is a dictionary or another type
                print("Received data is not a list")
        # Update the last update time to now
        update_time_sql = "UPDATE config SET last_out_update = %s,current_session = %s"
        params = (s_current_datetime,date_str)
        write_to_mysql(update_time_sql,params)
def query_mysql(sql, params=None):
    connection = connect_to_mysql()
    cursor = connection.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    # Fetch the results
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results
def write_to_mysql(sql,params=None):
    connection = connect_to_mysql()
    mycursor = connection.cursor()
    if params:
        if isinstance(params, list) and isinstance(params[0], (list, tuple)):
            # If params is a list of lists or a list of tuples, use executemany
            mycursor.executemany(sql, params)
        else:
            # Otherwise, use a single execute
            mycursor.execute(sql, params)
    else:
        mycursor.execute(sql)

    connection.commit()
    mycursor.close()
    connection.close()

def update_outs(hip_data):
    # Retrieve last out update time
    result = query_mysql("SELECT last_out_update FROM config")
    last_out_update = result[0][0] if result else None

    if not last_out_update:
        last_out_update = "1900-01-01"

    # Parse hip_data for new outs
    for record in hip_data:
        if record["out_date"] > last_out_update:
            insert_sql = """
            INSERT INTO outs (hip_number, out_date, session_date) 
            VALUES (%s, %s, %s)
            """
            params = (record["hip_number"], record["out_date"], record["session_date"])
            write_to_mysql(insert_sql, params)

    # Update the last update time to now
    update_time_sql = "UPDATE config SET last_out_update = NOW()"
    write_to_mysql(update_time_sql)
#Remember case when program is restarted no need to run fetch again
fetch_outs()


