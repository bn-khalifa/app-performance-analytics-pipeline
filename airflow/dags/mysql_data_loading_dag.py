from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import MySQLdb
@dag(
       dag_id="data_loading_workflow",
       schedule="0 0 * * *",
       default_args={
           "owner": "airflow",
           "start_date": datetime(2025, 10, 1),
           "retries": 1,
           "retry_delay": timedelta(minutes=2),
       },
       catchup=False
   )
def data_loading_workflow():
    create_apps_info_table = SQLExecuteQueryOperator(
        task_id="create_apps_info_table",
        sql="""
        CREATE TABLE IF NOT EXISTS apps_info (
            App VARCHAR(50) PRIMARY KEY,
            Category VARCHAR(50),
            Rating FLOAT,
            Reviews INT,
            Size VARCHAR(20),
            Installs VARCHAR(20),
            Type VARCHAR(10),
            Price FLOAT,
            Content_Rating VARCHAR(20),
            Genres VARCHAR(50)
            );
        """,
        conn_id="mysql_docker"
    )

    create_apps_info_temp_table = SQLExecuteQueryOperator(
        task_id="create_apps_info_temp_table",
        sql="""
        DROP TABLE IF EXISTS apps_info_temp;
        CREATE TABLE apps_info_temp (
            App VARCHAR(50), 
            Category VARCHAR(50),
            Rating FLOAT,
            Reviews INT,
            Size VARCHAR(20),
            Installs VARCHAR(20),
            Type VARCHAR(10),
            Price FLOAT,
            Content_Rating VARCHAR(20),
            Genres VARCHAR(50)
            );
        """,
        conn_id="mysql_docker"
        )
    
    @task
    def load_data():
        data_path = '/opt/airflow/datasets/googleplaystore.csv'

        # Connect directly with local_infile enabled
        conn = MySQLdb.connect(
            host="mysql_db",
            user="root",
            passwd="root",
            db="app_data",
            local_infile=1  # allows LOAD DATA LOCAL INFILE
        )
        cur = conn.cursor()

        cur.execute(f"""
            LOAD DATA LOCAL INFILE '{data_path}'
            INTO TABLE apps_info_temp
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
        """)

        conn.commit()
        cur.close()
        conn.close()


    @task
    def merge_data():
        mysql_hook = MySqlHook(mysql_conn_id="mysql_docker")
        conn = mysql_hook.get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            REPLACE INTO apps_info
            SELECT * FROM apps_info_temp;
        """)
        
        conn.commit()
        cur.close()
        conn.close()

    [create_apps_info_table, create_apps_info_temp_table] >> load_data() >> merge_data()

dag = data_loading_workflow()
