from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd

@dag(
    dag_id="mongo_reviews_loading",
    schedule="0 0 * * *", 
    default_args={
        "owner": "airflow",
        "start_date": datetime(2025, 10, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    catchup=False
)
def mongo_reviews_loading():
    @task
    def load_reviews():
        data_path = '/opt/airflow/datasets/googleplaystore_user_reviews.csv'
        df = pd.read_csv(data_path)

        client = MongoClient("mongodb://mongo_db:27017/")
        db = client["app_data"]
        collection = db["reviews"]

        records = df.to_dict(orient='records')
        collection.insert_many(records)
        client.close()
    

dag = mongo_reviews_loading()


