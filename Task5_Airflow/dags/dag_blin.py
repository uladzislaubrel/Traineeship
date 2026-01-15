from airflow.sdk import dag, task


@dag(
    schedule="@daily",
    description="Example dag",
    tags=["11212", "qweqweqwe"] 
)
def dag_blin():

    @task
    def task_a():
        print("task a")

    @task
    def task_b():
        print("task b")

    @task
    def task_c():
        print("task c")

    @task
    def task_d():
        print("task d")

    @task
    def task_e():
        print("task e")

    a = task_a()
    a >> task_b() >> task_c()
    a >> task_d() >> task_e()

dag_blin()