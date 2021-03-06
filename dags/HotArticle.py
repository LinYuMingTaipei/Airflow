from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime, timedelta
import pendulum
import requests
import json
import pandas as pd

local_tz = pendulum.timezone("Asia/Taipei")
default_args = {
    'owner': 'Joe Lin',
    'start_date': datetime(2020, 5, 1, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# PTT config
PTTUrl  = "https://moptt.azurewebsites.net/api/v2/hotpost?b=Gossiping&b=Boy-Girl&b=Beauty&b=marvel&b=WomenTalk&b=movie"
headers = Variable.get("User-Agent")

# MySQL config
mysqlhook = MySqlHook(mysql_conn_id="PTT")
cursor    = connection.cursor()

def create_table(**context):
    execution_date = context["execution_date"].strftime("%Y%m%d %H:%M:%S")
    table_name = f"HotArticle_{execution_date}"
    from tasks.ptt import create_table
    create_table(table_name)
    return table_name


def crawlPTT(**context):
    table_name = context['task_instance'].xcom_pull(task_ids='create_table')
    r = requests.get(PTTUrl,headers=headers)
    posts = json.loads(r.text)
    result = list()
    for post in posts:
        row = {}
        row["title"]  = post["title"]
        row["author"] = post["author"]
        row["board"]  = post["board"]
        row["hits"]   = post["hits"]
        row["url"]    = post["url"]
        row["timestamp"]   = post["timestamp"]
        row["description"] = post["description"]
        result.append(row)

    result = pd.DataFrame(result)
    result.to_sql(name=table_name, con=cursor, if_exists='replace', index=False)


with DAG('HotArticle', default_args=default_args,schedule_interval='0 9 1 * *') as dag:
    crawlPTT = PythonOperator(
        task_id = 'crawlPTT',
        python_callable = crawlPTT,
        provide_context = True
    )

    createTable = PythonOperator(
        task_id = "ceateTable",
        python_callable = ceate_table,
        provide_context = True
    )

    createTable >> crawlPTT
