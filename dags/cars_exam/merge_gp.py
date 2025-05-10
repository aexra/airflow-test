from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from xml.etree import ElementTree as ET

import pandas as pd
import requests

@dag(
  dag_id="merge_cars",
  start_date=days_ago(0),
  description="Переводит стоимость в рублевый эквивалент по актуальному на дату создания файла курсу ЦБ и складывает эту информацию в таблицу в GreenPlum",
  schedule_interval=None,
  tags=["cars_test"]
)
def merge_gp():
  
  @task
  def extract_cars(execution_date: datetime) -> pd.DataFrame: 
    hook = S3Hook(aws_conn_id='minio_conn')
    file = hook.download_file(f"cars-{execution_date.date()}.csv", "cars")
    df = pd.read_csv(file)
    print(df.head())
    return df
  
  @task
  def extract_exchange_rates() -> dict[str, float]:
    r = requests.get('https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021')
    root = ET.fromstring(r.text)

    valutes: dict[str, float] = dict()

    for valute in root.findall('Valute'):
      char_code = valute.find('CharCode').text      
      unit_rate = valute.find('VunitRate').text
      valutes[char_code] = unit_rate
      
    return valutes
  
  @task
  def transform_data(cars: pd.DataFrame, rate: dict[str, float]):
    pass
  
  @task
  def load_data():
    pass
  
  cars = extract_cars()
  rates = extract_exchange_rates()
  transform = transform_data(cars, rates)
  load = load_data()
  
  [cars, rates] >> transform >> load
  
dag = merge_gp()