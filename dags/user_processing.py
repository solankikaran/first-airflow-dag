from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
import requests

@dag
def user_processing():

    # task 1 - create table in a PostgreSQLDB
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR (255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """        
    )

    # task 2 - verifies if API is available using Sensor and gets the response 
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None

        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    # task 3 - extract user from API using @task
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "first_name": fake_user["personalInfo"]["firstName"],
            "last_name": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"]
        }
    
    fake_user = is_api_available()
    user_info = extract_user(fake_user)

user_processing()