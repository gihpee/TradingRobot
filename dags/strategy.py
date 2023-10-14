from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def strategy():
    print('hello wrld')


strategy_dag = DAG(
    "strategy",
    description='strategy',
    schedule_interval="0 0 * * *",
    start_date=days_ago(0, 0, 0, 0, 0),
    tags=['strategy'],
    doc_md='strategy'
)

strategy_operator = PythonOperator(
    task_id='strategy',
    python_callable=strategy,
    dag=strategy_dag
)

strategy_operator

