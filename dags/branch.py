from airflow.sdk import dag, task

@dag
def branch():

    @task
    def a():
        return 1

    @task.branch
    def b(val):
        if val <= 1:
            return "c"
        return "e"

    @task
    def c(val):
        print(f"value is {val}")

    @task
    def d(val):
        print(f"value is {val}")

    val = a()
    b(val) >> [c(val), d(val)]

branch()