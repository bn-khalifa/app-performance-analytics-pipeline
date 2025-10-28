from airflow.decorators import dag, task
from datetime import datetime, timedelta
import duckdb as ddb
import pandas as pd
import MySQLdb
from pymongo import MongoClient

DUCKDB_PATH = "/opt/airflow/datasets/app_analytics.duckdb"
@dag(
    dag_id="extract_to_duckdb",
    start_date=datetime(2025, 10, 1),
    schedule="@weekly", 
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    catchup=False
)
def extract_to_duckdb():
    @task
    def extract_and_load():
        mysql_conn = MySQLdb.connect(
            host="mysql_db",
            user="root", 
            passwd="root",
            db="app_data",
        )
        apps_df = pd.read_sql("select * from apps_info;", con=mysql_conn)
        mysql_conn.close()

        mongo_client = MongoClient("mongodb://mongo_db:27017/")
        reviews_df = pd.DataFrame(list(mongo_client["app_data"]["reviews"].find()))
        mongo_client.close()

        con = ddb.connect(DUCKDB_PATH)
        con.register("apps", apps_df)
        con.register("reviews", reviews_df)
        con.execute("CREATE OR REPLACE TABLE apps_info AS SELECT * FROM apps")
        con.execute("CREATE OR REPLACE TABLE reviews AS SELECT * FROM reviews")
        con.close()
        

dag = extract_to_duckdb()
        
