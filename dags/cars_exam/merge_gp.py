from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
  dag_id="merge_cars",
  start_date=days_ago(0),
  description="Переводит стоимость в рублевый эквивалент по актуальному на дату создания файла курсу ЦБ и складывает эту информацию в таблицу в GreenPlum",
  schedule_interval=None,
  tags=["cars_test"]
)
def merge_gp():
  
  @task
  def extract_data():
    pass
  
  @task
  def transform_data():
    pass
  
  @task
  def load_data():
    pass
  
  extract = extract_data()
  transform = transform_data()
  load = load_data()
  
  extract >> transform >> load
  
dag = merge_gp()