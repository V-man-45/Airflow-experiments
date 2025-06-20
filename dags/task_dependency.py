import logging
import pendulum

from airflow.decorators import dag, task

@dag(
    start_date=pendulum.now(),
    schedule_interval='@hourly',
)

def task_dependencies():
    @task()
    def hello_world_task():
        logging.info("Hello World!")
    
    @task()
    def addition_task(first, second):
        logging.info(f" {first} + {second} = {first + second}")
        return first + second
    
    @task()
    def subtraction_task(first, second):
        logging.info(f" {first} - {second} = {first - second}")
        return first - second
    
    @task()
    def division_task(first, second):
        logging.info(f" {first} / {second} = {first / second}")
        return first / second
    
    hello = hello_world_task()

    first_addition = addition_task(9, 4)

    first_subtraction = subtraction_task(5, 4)

    first_division = division_task(10, 2)

    second_addition = addition_task(6, 5)

    second_subtraction = subtraction_task(7, 3)

    firstsum_secondsubtraction = division_task(first_addition, second_subtraction)

    hello >> first_addition >> first_subtraction >> first_division >> second_addition >> second_subtraction >> firstsum_secondsubtraction

# task_dependencies represents the invocation of the task_dependencies
task_dependencies_dag = task_dependencies()

