from io import StringIO
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import datetime

from pandas import DataFrame

@dag(
  dag_id="s3_save_current",
  start_date=days_ago(0),
  description="Заполняет БД (псевдо)актуальными значениями",
  schedule_interval="0 0 * * 1-5",
  tags=["cars_test"]
)
def save():
  
  @task
  def extract_cars():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    query = """
      SELECT * FROM cars;
    """

    logging.info(f"SQL query for DB: {query}")

    try:
      cur.execute(query)
      result = cur.fetchall()
      columns = [desc[0] for desc in cur.description]
    except Exception as e:
      conn.rollback()
      logging.error(f"Exception raised while attempting to execute SQL: {e}")
      raise
    finally:
      cur.close()
      conn.close()

    return result, columns

  @task
  def transform_cars(cars_data: tuple[list[tuple], list]):
    cars, columns = cars_data
    df = DataFrame(cars, columns=columns)
    date = datetime.datetime.now()
    df["collected_at"] = [date] * df.shape[0]
    return df, date

  @task
  def load_cars(data: tuple[DataFrame, datetime.date]):
    df, date = data
    hook = S3Hook(aws_conn_id='minio_conn')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
      hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f"cars-{date}.csv",
        bucket_name="cars",
        replace=True
      )
    except Exception as e:
      logging.error(f"Failed to upload to S3: {e}")
      raise

  extracted = extract_cars()
  transformed = transform_cars(extracted)
  loaded = load_cars(transformed)

dag = save()