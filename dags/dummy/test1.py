from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

DEFAULT_ARGS = {
  'owner': 'aexra'
}

with DAG(
  dag_id="test1",
  description="my first dag",
  tags=["test"],
  schedule_interval='@daily',
  start_date=days_ago(2),
  default_args=DEFAULT_ARGS,
  max_active_runs=1
) as dag:
  def i_need_a_bullets():
    logging.warning('I NEED A BULLETS')
    hook = S3Hook(aws_conn_id='minio_conn')
    bucket = hook.check_for_bucket('cars')
    print(f"\nAvailable bucket: {bucket}\n\n")


  task1 = PythonOperator(
    python_callable=i_need_a_bullets,
    task_id="aboba"
  )

  task2 = BashOperator(
    bash_command='''echo "Hello, world!"''',
    task_id="bebebe"
  )

  task1 >> task2

