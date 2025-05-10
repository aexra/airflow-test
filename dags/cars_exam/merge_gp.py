from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from xml.etree import ElementTree as ET

import pandas as pd
import requests
import logging

@dag(
  dag_id="merge_cars",
  start_date=days_ago(0),
  description="Переводит стоимость в рублевый эквивалент по актуальному на дату создания файла курсу ЦБ и складывает эту информацию в таблицу в GreenPlum",
  schedule_interval=None,
  tags=["cars_test"]
)
def merge_gp():
  
  @task
  def extract_cars() -> pd.DataFrame: 
    hook = S3Hook(aws_conn_id='minio_conn')
    
    files = hook.list_keys("cars")
    
    if not files:
      raise ValueError(f"Файлы не найдены в бакете 'cars'")
    
    files_with_metadata = [(key, hook.get_key(key, 'cars').last_modified) for key in files]
    
    # Сортируем по дате изменения (последний файл будет первым)
    files_with_metadata.sort(key=lambda x: x[1], reverse=True)
    latest_file_key = files_with_metadata[0][0]
    
    file = hook.download_file(latest_file_key, "cars")
    df = pd.read_csv(file)
    df["source_filename"] = latest_file_key
    return df
  
  @task
  def extract_exchange_rates() -> dict[str, float]:
    r = requests.get('https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021')
    root = ET.fromstring(r.text)

    valutes: dict[str, float] = dict()

    for valute in root.findall('Valute'):
      char_code = valute.find('CharCode').text
      unit_rate = float(valute.find('VunitRate').text.replace(',', '.'))
      valutes[char_code] = unit_rate
      
    return valutes
  
  @task
  def transform_data(cars: pd.DataFrame, rate: dict[str, float]) -> pd.DataFrame:
    cars["ex_rate"] = cars["currency"].map(rate)
    cars["rub"] = cars["price"] * cars["ex_rate"]
    return cars
  
  @task
  def load_data(cars: pd.DataFrame):
    hook = PostgresHook(postgres_conn_id="gp_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
      dim_query = f"""
      INSERT INTO dim_cars (car_sk, mark, model, engine_volume, year_of_manufacture) VALUES
      {",\n".join([f"('{c['id']}', '{c['mark']}', '{c['model']}', {c['engine_volume']}, {c['year']})" for i, c in cars.iterrows()])}
      ON CONFLICT (car_sk) DO NOTHING;
      """
      
      logging.info(f"Refreshing cars dimensions table")
      logging.info(f"Executing query: {dim_query}")
      cur.execute(dim_query)
      
      close_old_fact_query = f"""
      UPDATE fact_cars
      SET 
        is_current = false,
        effective_to = CURRENT_TIMESTAMP
      """
      
      logging.info(f"Fixing cars facts table")
      logging.info(f"Executing query: {close_old_fact_query}")
      cur.execute(close_old_fact_query)
      
      fact_query = f"""
      INSERT INTO fact_cars (car_sk, price_foreign, currency_code_foreign, price_rub, ex_rate, source_filename) VALUES
      {",\n".join([f"('{c['id']}', '{c['price']}', '{c['currency']}', '{c['rub']}', '{c['ex_rate']}', '{c['source_filename']}')" for _, c in cars.iterrows()])}
      """
      
      logging.info(f"Updating cars facts table")
      logging.info(f"Executing query: {fact_query}")
      cur.execute(fact_query)
      
      conn.commit()
      
      logging.info(f"Successfully inserted {len(cars)} records")
    except Exception as e:
      conn.rollback()
      logging.error(f"Error inserting data: {str(e)}")
      raise
    finally:
      cur.close()
      conn.close()
  
  cars = extract_cars()
  rates = extract_exchange_rates()
  transform = transform_data(cars, rates)
  load = load_data(transform)
  
  [cars, rates] >> transform >> load
  
dag = merge_gp()