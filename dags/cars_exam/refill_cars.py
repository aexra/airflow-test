from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import numpy as np
import pandas as pd
import time
import xml.etree.ElementTree as ET
import requests

CAR_MIN_PRICE = 10_000
CAR_MAX_PRICE = 50_000
CARS_PER_REFILL=10

@dag(
    dag_id="refill_cars",
    start_date=days_ago(0),
    description="Заполняет БД (псевдо)актуальными значениями",
    schedule_interval=None,
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
      year integer NOT NULL,
      currency text NOT NULL,
      price numeric NOT NULL
    )
    """
  )

  @task
  def extract_random_cars():
    df = pd.read_csv('assets/cars.csv', sep=';')
    rand = df.sample(CARS_PER_REFILL, random_state=int(time.time())).copy()
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
      TRUNCATE TABLE cars;
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

  extract = extract_random_cars()
  extract_curs = extract_currencies()
  transform = transform_cars(extract, extract_curs)
  load = load_cars(transform)

  create_table >> [extract, extract_curs] >> transform >> load

cars_dag = refill_cars()