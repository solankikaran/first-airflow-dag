from airflow.sdk import dag, task

@dag
def xcom_dag():

    @task
    def t1():
        value_1 = 42
        value_2 = "some_string"
        value_3 = 80

        return {
            "value_1" : value_1,
            "value_2" : value_2,
            "value_3" : value_3
        }

    @task
    def t2(d):
        return d["value_1"], d["value_2"], d["value_3"]

    val = t1()
    t2(val)

xcom_dag()