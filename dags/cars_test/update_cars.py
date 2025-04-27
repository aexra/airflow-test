from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging

@dag(
    dag_id="refill_cars",
    start_date=days_ago(2),
    description="Заполняет БД (псевдо)актуальными значениями",
    schedule_interval="@once",
    tags=["cars_test"]
)
def refill_cars():
  
  create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id='postgres_conn',
    database="db",
    sql="""
    CREATE TABLE IF NOT EXISTS Cars (
      id serial PRIMARY KEY,
      mark text NOT NULL,
      model text NOT NULL,
      engine_volume numeric NOT NULL,
      published_at integer NOT NULL,
      price_usd numeric NOT NULL
    )
    """
  )

  @task
  def extract_random_cars():
    return ['abobabebebe']

  @task
  def transform_cars(cars):
    return []

  @task
  def load_cars(cars):
    logging.info(f"\n----------------------\n{cars}\n----------------------")

  extract = extract_random_cars()
  transform = transform_cars(extract)
  load = load_cars(transform)

  create_table >> extract >> transform >> load

cars_dag = refill_cars()