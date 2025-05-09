import time
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import numpy as np
import pandas as pd
import requests
import xml.etree.ElementTree as ET

from dags.cars_exam.conf import CARS_UPDATE_PER_UPDATE
from dags.cars_exam.conf import CARS_DELETE_PER_UPDATE

@dag(
    dag_id="update_cars",
    start_date=days_ago(0),
    description="Обновляет БД (псевдо)актуальными значениями",
    schedule_interval="0 23 * * 1-5",
    tags=["cars_test"]
)
def update_cars():

  @task
  def extract_random_cars():
    df = pd.read_csv('assets/cars.csv', sep=';')
    rand = df.sample(CARS_PER_UPDATE, random_state=int(time.time())).copy()
    return rand
  
  @task
  def extract_currencies(cars):
    r = requests.get('https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021')
    root = ET.fromstring(r.text)

    codes = []

    for valute in root.findall('Valute'):
      char_code = valute.find('CharCode').text      
      codes.append(char_code)
      
    return cars, codes

  @task
  def transform_cars(cars, codes):
    cars['price_usd'] = np.random.randint(10_000, 50_000, size=cars.shape[0])
    cars['codes'] = np.random.choice(codes, size=cars.shape[0])
    
    return cars

  @task
  def load_cars(cars: pd.DataFrame):
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
      cur.execute("SELECT car_id FROM cars ORDER BY RANDOM() LIMIT 1")
      random_car_id = cur.fetchone()[0]
      
      query = f"""
      TRUNCATE TABLE cars;
      INSERT INTO cars (mark, model, engine_volume, year, currency, price) VALUES
      {",\n".join([f"('{c['mark']}', '{c['model']}', {c['engine_volume']}, {c['year']}, {c['currency']}, {c['price']})" for i, c in cars.iterrows()])};
      """
      
      print("Executing query:", query)
      
      cur.execute(query)
      conn.commit()
      
      logging.info(f"Successfully inserted {len(cars)} records")
    except Exception as e:
      conn.rollback()
      logging.error(f"Error inserting data: {str(e)}")
      raise
    finally:
      cur.close()
      conn.close()

  extract = extract_random_cars()
  extract_curs = extract_currencies(extract)
  transform = transform_cars(extract_curs)
  load = load_cars(transform)

  extract >> extract_curs >> transform >> load

cars_dag = update_cars()