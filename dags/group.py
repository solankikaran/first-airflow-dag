from airflow.sdk import dag, task, task_group

@dag
def group():

    @task
    def a():
        return 42
    
    @task_group(default_args={"retries": 2})
    def my_group(val):

        @task
        def b(my_val):
            print(my_val, my_val+8)
        
        @task
        def c():
            print("c")
        
        b(val) >> c()
    
    val = a()
    my_group(val)

group()