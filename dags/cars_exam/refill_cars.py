from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import numpy as np
import pandas as pd
import time

CARS_PER_REFILL=10

@dag(
    dag_id="refill_cars",
    start_date=days_ago(2),
    description="Заполняет БД (псевдо)актуальными значениями",
    schedule_interval="0 0 * * 1-5",
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
    df = pd.read_csv('assets/cars.csv', sep=';')
    rand = df.sample(CARS_PER_REFILL, random_state=int(time.time())).copy()
    return rand

  @task
  def transform_cars(cars):
    cars['price_usd'] = np.random.randint(10_000, 50_000, size=cars.shape[0])
    cars['id'] = range(1, cars.shape[0] + 1)
    return cars

  @task
  def load_cars(cars: pd.DataFrame):
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        query = f"""
        TRUNCATE TABLE cars;
        INSERT INTO cars (id, mark, model, engine_volume, published_at, price_usd) VALUES
        {",\n".join([f"({c['id']}, '{c['mark']}', '{c['model']}', {c['engine_volume']}, {c['published_at']}, {c['price_usd']})" for i, c in cars.iterrows()])};
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
  transform = transform_cars(extract)
  load = load_cars(transform)

  create_table >> extract >> transform >> load

cars_dag = refill_cars()