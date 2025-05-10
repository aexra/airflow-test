from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

import pandas as pd

@dag(
  dag_id="merge_cars",
  start_date=days_ago(0),
  description="Переводит стоимость в рублевый эквивалент по актуальному на дату создания файла курсу ЦБ и складывает эту информацию в таблицу в GreenPlum",
  schedule_interval=None,
  tags=["cars_test"]
)
def merge_gp():
  
  @task
  def extract_cars(execution_date: datetime):
    hook = S3Hook(aws_conn_id='minio_conn')
    file = hook.download_file(f"cars-{execution_date.date()}.csv", "cars")
    df = pd.read_csv(file)
    print(df.head())
    return df
  
  @task
  def extract_exchange_rate():
    pass
  
  @task
  def transform_data(cars: pd.DataFrame, rate: tuple[str, float]):
    pass
  
  @task
  def load_data():
    pass
  
  cars = extract_cars()
  rate = extract_exchange_rate()
  transform = transform_data(cars, rate)
  load = load_data()
  
  [cars, rate] >> transform >> load
  
dag = merge_gp()