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

CAR_MIN_PRICE = 10_000
CAR_MAX_PRICE = 50_000
CARS_CREATE_PER_UPDATE=1
CARS_DELETE_PER_UPDATE=1
CARS_UPDATE_PER_UPDATE=2

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
    rand = df.sample(CARS_CREATE_PER_UPDATE, random_state=int(time.time())).copy()
    return rand
  
  @task
  def extract_currencies():
    r = requests.get('https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021')
    root = ET.fromstring(r.text)

    codes = []

    for valute in root.findall('Valute'):
      char_code = valute.find('CharCode').text      
      codes.append(char_code)
      
    return codes

  @task
  def transform_cars(cars, codes):
    np.random.seed(int(time.time()))
    
    cars['price'] = np.random.randint(CAR_MIN_PRICE, CAR_MAX_PRICE, size=cars.shape[0])
    cars['currency'] = np.random.choice(codes, size=cars.shape[0])
    
    return cars

  @task
  def load_cars(cars: pd.DataFrame):
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:      
      query = f"""
      INSERT INTO cars (mark, model, engine_volume, year, currency, price) VALUES
      {",\n".join([f"('{c['mark']}', '{c['model']}', {c['engine_volume']}, {c['year']}, '{c['currency']}', {c['price']})" for i, c in cars.iterrows()])};
      """
      
      logging.info(f"Executing query: {query}")
      
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
      
  @task
  def delete_cars():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
      cur.execute(f"SELECT id FROM cars ORDER BY RANDOM() LIMIT {CARS_DELETE_PER_UPDATE}")
      random_car_id = cur.fetchone()[0]
      
      delete_query = f"DELETE FROM cars WHERE id = {random_car_id}"
      logging.info(f"Executing delete query: {delete_query}")
      
      cur.execute(delete_query)
      conn.commit()
      
      logging.info(f"Successfully deleted {CARS_DELETE_PER_UPDATE} records")
    except Exception as e:
      conn.rollback()
      logging.error(f"Error deleting data: {str(e)}")
      raise
    finally:
      cur.close()
      conn.close()

  @task
  def update_cars():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    np.random.seed(int(time.time()))

    try:
      cur.execute(f"SELECT id FROM cars ORDER BY RANDOM() LIMIT {CARS_UPDATE_PER_UPDATE}")
      random_car_id = cur.fetchone()[0]
      
      update_query = f"""
      UPDATE cars 
      SET price = {np.random.randint(CAR_MIN_PRICE, CAR_MAX_PRICE)}
      WHERE id = {random_car_id};
      """
      logging.info(f"Executing update query: {update_query}")
      
      cur.execute(update_query)
      conn.commit()
      
      logging.info(f"Successfully updated {CARS_UPDATE_PER_UPDATE} records")
    except Exception as e:
      conn.rollback()
      logging.error(f"Error updating data: {str(e)}")
      raise
    finally:
      cur.close()
      conn.close()

  extract = extract_random_cars()
  extract_curs = extract_currencies()
  transform = transform_cars(extract, extract_curs)
  delete = delete_cars()
  update = update_cars()
  load = load_cars(transform)

  [extract, extract_curs] >> transform >> [delete, update, load]

cars_dag = update_cars()