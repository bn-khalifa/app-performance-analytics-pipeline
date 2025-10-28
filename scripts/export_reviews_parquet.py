import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://mongo_db:27017/")
db = client["app_data"]

df = pd.DataFrame(list(db["reviews"].find()))
df = df.drop(columns=["_id"], errors="ignore")
df.to_parquet("/opt/airflow/datasets/reviews.parquet", index=False)

print("Exported reviews to Parquet")