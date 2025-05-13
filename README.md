## ТЗ

Исходный файл с ТЗ лежит [здесь](./test.md)

##### Основное задание
> В бакет на S3 в будние дни по расписанию кладется файл csv с содержимым: автомобиль, стоимость в иностранной валюте, дата актуальности всего файла. Характеристики автомобилей: марка, модель, объем двигателя, год выпуска. С каждым файлом информация может изменяться: добавляются строки для других автомобилей, удаляются строки для уже бывших или же изменяется стоимость на них.
>
> Написать DAG для Apache Airflow, который переводит стоимость в рублевый эквивалент по актуальному на дату создания файла курсу ЦБ и складывает эту информацию в таблицу в GreenPlum. DAG должен работать с понедельника по субботу, но не по воскресеньям. В субботу необходимо использовать курс валют, актуальный на минувшую пятницу.
>
> Курс валют можно скачать с сайта ЦБ на нужную дату https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021

##### Задание 1
> Используя свою первую реализацию задачи, проработать отказоустойчисвость задействованных элементов КХД следующим образом: указать в docstrings к каждому методу/таске, где необходимо, что может пойти не так (и нарушить DataPipeline); предлжить способ устранения данной ошибки (так, чтобы DataPipeline был стабилен).

##### Задание 2
> Так же, в docstrings, пометить элементы, несоответствующие основным принципам КХД. Переработать для соответствия.

##### Задание 3
> Спроектировать вьюху для аналитического отчета "Динамика цены автомобиля за месяц", с возможностью составить аналитику в разрезе марки или модели или года выпуска. Описать структуру в любой удобной форме.

## Реализация

[Итоговый DAG по заданию](./dags/cars_exam/merge_gp.py)

##### Задание 1

> Используя свою первую реализацию задачи, проработать отказоустойчисвость задействованных элементов КХД следующим образом: указать в docstrings к каждому методу/таске, где необходимо, что может пойти не так (и нарушить DataPipeline); предлжить способ устранения данной ошибки (так, чтобы DataPipeline был стабилен).

Глобально исходный граф был полностью переработан, но на примере первой реализации выделим что может пойти не так и как это можно исправить, а также исправим в новой реализации.

###### Получение курса валют

```py
# Получение курса вылюты с ЦБ
def get_exchange_rate(**context):
  """
  Получает курсы валют с ЦБ РФ
  Raises:
    ValueError: Курс доллара США не найден
    TypeError: ElementTree требует bytes-like object, но получает что-то другое
  Проблемы в общем:
    1. Задача реализована только для одной валюты
    2. Ошибки в получении курсов приводят к завершению работы DAG
    3. Неограниченный таймаут в requests
  """
  execution_date = context['execution_date']

  # Если сегодня суббота, возьмем целевую дату на день раньше
  if execution_date.weekday() == 5:
      target_date = execution_date - timedelta(days=1)
  else:
      target_date = execution_date

  date_str = target_date.strftime("%d/%m/%Y")

  url = f"https://cbr.ru/scripts/xml_daily.asp?date_req={date_str}"

  response = requests.get(url)

  root = ET.fromstring(response.content)

  for currency in root.findall(".//Valute"):
      if currency.find("CharCode").text == "USD":  # Курс доллара (в ТЗ не было указано какая именно иностранная валюта хранится в csv)
          rate = float(currency.find("Value").text.replace(",", "."))
          return rate

  raise ValueError("USD rate not found")
```

###### Решение

1. Будем поддерживать все доступные валюты, а этой таской будем собирать словарь <валюта: курс> для всех полученных из XML.
2. Введем понятие резервного файла - будем размещать в нем данные, успешно полученные при последнем выполнении дага, и использовать данные из него при неудачных запросах к ЦБ.
3. Добавим таймаут в 10 секунд

Резервный файл положим [сюда](./assets/fallbackExchangeRate.xml)

```py
@task
def extract_exchange_rates() -> dict[str, float]:
  """
  Получает актуальные курсы валют с сайта ЦБ РФ на указанную дату. Если дата - суббота, использует данные с минувшей пятницы.
  Returns:
    Словарь с курсами валют, где ключ - код валюты (например, 'USD'), а значение - курс за рубль.
  Raises:
    ValueError:
      Если запрос вернул пустой список валют
    Exception:
      Невозможно получить данные о курсах ни из одного источника
  """
  
  fallback = 'assets/fallbackExchangeRate.xml'
  
  def parse_rate_from_xml(xml_content: str) -> dict[str, float]:
    """Парсит курс USD из XML-контента."""
    try:
        root = ET.fromstring(xml_content)
        valutes: dict[str, float] = dict()

        for valute in root.findall('Valute'):
          char_code = valute.find('CharCode').text
          unit_rate = float(valute.find('VunitRate').text.replace(',', '.'))
          valutes[char_code] = unit_rate
        
        if len(valute.keys()) < 1:
          raise ValueError("Currencies list is empty")  
        
        return valutes
    except (ET.ParseError, AttributeError, ValueError) as e:
        logging.error(f"Error parsing XML: {e}")
        return {}
  
  try:
    date = datetime.now()
    
    if date.weekday() >= 5:
      days_to_friday = date.weekday() - 4
      date = date - timedelta(days=days_to_friday)
      
    date_str = date.strftime('%d/%m/%Y')
    
    # Установим таймаут для запроса
    r = requests.get(f'https://cbr.ru/scripts/xml_daily.asp?date_req={date_str}', timeout=10)

    # Вызовем исключение если не смогли получить данные
    r.raise_for_status()
    
    if rate := parse_rate_from_xml(r.text):
      # Обновляем fallback файл при успешном получении
      with open(fallback, 'w') as f:
          f.write(r.text)
      return rate

  except Exception as e:
    # Если произошла ошибка в получении данных,
    # попробуем получить курс из резервного файла
    try:
      with open(fallback, 'r') as f:
        return parse_rate_from_xml(f.read())
    except:
      raise Exception(f"Cannot get exchange rates from any source. Inner exception: {e}")
```

###### Загрузка файла из S3

```py
def download_csv_from_s3(bucket_name, object_key, local_path, **kwargs):
  """
  Извлекает файл с машинами из S3
  Проблемы:
    1. Должным образом не обрабатывается ошибка отсутствия ключа (бакет/файл не существует).
    2. Текущая реализация в принципе предполагает наличие только одного файла в бакете, что не позволит просматривать старые записи.
  """
  s3 = S3Hook(aws_conn_id='minio_default')
  s3.get_key(object_key, bucket_name).download_file(local_path)
```

###### Решение

1. Первым делом осуществим поиск ключей в S3, и вызовем исключение если их список пуст (или бакет не существует, или бакет пустой).
2. Выполним сортировку ключей по дате их изменения и используем последний измененный для дальнейшей работы. Это будет работать в случае, если DAG выполняется строго по расписанию, без ручных вызовов, следовательно, без надобности обрабатывать все необработанные файлы.

```py
@task
def extract_cars() -> pd.DataFrame: 
  """
  Извлекает последний загруженный CSV-файл с данными об автомобилях из S3 (MinIO).
  Returns:
    pd.DataFrame: DataFrame с данными об автомобилях, включая имя исходного файла.
  Raises:
    ValueError: Если в бакете 'cars' не найдено ни одного файла.
  """
  
  hook = S3Hook(aws_conn_id='minio_conn')
  
  files = hook.list_keys("cars")
  
  if not files:
    raise ValueError("Bucket 'cars' does not exists or does not contain files")
  
  files_with_metadata = [(key, hook.get_key(key, 'cars').last_modified) for key in files]
  
  # Сортируем по дате изменения (последний файл будет первым)
  files_with_metadata.sort(key=lambda x: x[1], reverse=True)
  latest_file_key = files_with_metadata[0][0]
  
  file = hook.download_file(latest_file_key, "cars")
  df = pd.read_csv(file)
  df["source_filename"] = latest_file_key
  return df
```

###### Запись в КХД

```py
def process_and_load_to_db(csv_path, exchange_rate, **kwargs):
  """
  Конвертирует типы где необходимо в числовые, очищает таблицу и заполняет актуальными данными
  Проблемы:
    1. Это вообще не принцип работы КХД. Сейчас эта таблица подразумевает хранение актуальной информации, следовательно нет никакого историзма.
    2. Не обрабатываются исключения провайдера PostgreSQL.
  """
  df = pd.read_csv(csv_path, sep=';')
  df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
  df['price_rub'] = df['price_usd'] * float(exchange_rate)

  conn = psycopg2.connect(**PG_CONN_PARAMS)
  cur = conn.cursor()

  cur.execute('TRUNCATE TABLE cars;')

  for _, row in df.iterrows():
      cur.execute(
          """
          INSERT INTO cars (brand, model, engine_capacity, prod_year, price)
          VALUES (%s, %s, %s, %s, %s);
          """,
          (row['brand'], row['model'], row['engine_capacity'], row['prod_year'], row['price_rub'])
      )

  conn.commit()
  cur.close()
  conn.close()
```

###### Решение

1. Полностью переработаем принцип записи. 
   - Минимизируем затраты памяти разделив записи на таблицу фактов и таблицу измерений (fact_cars, dim_cars) для логически неизменяемых полей: марка, модель, год производства.
   - При получении информации о новых авто будем заносить их атрибуты в dim_cars, и создавать первую запись об их стоимости в fact_cars, указывая `is_current = True` и `effective_to = infinity`, что будет указывать на актуальность кортежа.
   - Если в полученных данных произошли изменения (авто "пропало" из csv или цена на него изменилась) - будем указывать у последней записи `is_current = False` и `effective_to = CURRENT_TIMESTAMP`, что будет указывать на устаревшие кортежи.
2. Добавим обработку исключений для SQL запросов.

```py
@task
def load_data(cars: pd.DataFrame):
  """
  Загружает данные об автомобилях в GreenPlum, обновляя таблицу измерений (dim_cars)
  и таблицу фактов (fact_cars) с историей изменений цен.
  Args:
    cars: DataFrame с преобразованными данными об автомобилях.
  Note:
    - Для dimension-таблицы используется INSERT ON CONFLICT DO NOTHING чтобы не дублировать авто по ID.
    - В fact-таблице сначала закрываются старые записи (is_current = false, effective_to = CURRENT_TIMESTAMP), затем добавляются новые.
  """
      
  def validate_filename(cur) -> bool:
    validation_query = f"""
    SELECT (source_filename) FROM public."fact_cars" fs
    ORDER BY fs."source_filename" DESC
    LIMIT 1
    """
    
    logging.info(f"Validating timestamps")
    # logging.info(f"Executing query: {validation_query}")
    cur.execute(validation_query)
    result = cur.fetchone()
    if (result):
      filename = result[0]
      if filename > cars["source_filename"][0]:
        raise ValueError("The file from S3 is older than used in facts table")
    
  def update_dim_cars(cur) -> None:
    dim_query = f"""
    INSERT INTO public."dim_cars" (car_sk, mark, model, engine_volume, year_of_manufacture) VALUES
    {",\n".join([f"('{c['id']}', '{c['mark']}', '{c['model']}', {c['engine_volume']}, {c['year_of_manufacture']})" for i, c in cars.iterrows()])}
    ON CONFLICT (car_sk) DO NOTHING;
    """
    
    logging.info(f"Refreshing cars dimensions table")
    # logging.info(f"Executing query: {dim_query}")
    cur.execute(dim_query)
    
  def close_old_fact_cars(cur) -> None:
    # Создаем временную таблицу с новыми данными
    temp_table_query = """
    CREATE TEMP TABLE temp_cars_data AS
    SELECT 
        car_sk, 
        price_foreign, 
        currency_code_foreign, 
        price_rub, 
        ex_rate
    FROM (VALUES
    """ + ",\n".join([
        f"({c['id']}, {c['price']}, '{c['currency']}', {c['rub']}, {c['ex_rate']})" 
        for _, c in cars.iterrows()
    ]) + """) AS t(car_sk, price_foreign, currency_code_foreign, price_rub, ex_rate);
    """
    
    # Закрываем записи: 1) где изменились данные, 2) которых нет в новых данных
    update_query = """
    -- Закрываем записи с измененными данными
    UPDATE public."fact_cars" fc
    SET 
        is_current = false,
        effective_to = CURRENT_TIMESTAMP
    FROM temp_cars_data temp
    WHERE 
        fc.car_sk = temp.car_sk
        AND fc.is_current = true
        AND (
            fc.price_foreign != temp.price_foreign
            OR fc.currency_code_foreign != temp.currency_code_foreign
            OR fc.price_rub != temp.price_rub
            OR fc.ex_rate != temp.ex_rate
        );

    -- Закрываем записи для машин, которых нет в новых данных
    UPDATE public."fact_cars" fc
    SET 
        is_current = false,
        effective_to = CURRENT_TIMESTAMP
    WHERE 
        fc.is_current = true
        AND NOT EXISTS (
            SELECT 1 FROM temp_cars_data temp 
            WHERE temp.car_sk = fc.car_sk
        );

    DROP TABLE temp_cars_data;
    """
    
    logging.info(f"Fixing cars facts table")
    # logging.info(f"Executing query: {close_old_fact_query}")
    cur.execute(temp_table_query)
    cur.execute(update_query)
  
  def add_new_fact_cars(cur) -> None:
    # Вставляем только записи для машин, у которых изменились данные
    fact_query = """
    INSERT INTO public."fact_cars" 
        (car_sk, price_foreign, currency_code_foreign, price_rub, ex_rate, source_filename)
    SELECT 
        t.car_sk, 
        t.price_foreign, 
        t.currency_code_foreign, 
        t.price_rub, 
        t.ex_rate,
        %s
    FROM (
        VALUES
    """ + ",\n".join([
        f"({c['id']}, {c['price']}, '{c['currency']}', {c['rub']}, {c['ex_rate']})" 
        for _, c in cars.iterrows()
    ]) + """
    ) AS t(car_sk, price_foreign, currency_code_foreign, price_rub, ex_rate)
    LEFT JOIN public."fact_cars" fc ON 
        t.car_sk = fc.car_sk 
        AND fc.is_current = true
    WHERE 
        fc.car_sk IS NULL
        OR (
            t.price_foreign != fc.price_foreign
            OR t.currency_code_foreign != fc.currency_code_foreign
            OR t.price_rub != fc.price_rub
            OR t.ex_rate != fc.ex_rate
        );
    """
    
    logging.info(f"Updating cars facts table")
    # logging.info(f"Executing query: {fact_query}")
    cur.execute(fact_query, (cars["source_filename"][0],))
        
  hook = PostgresHook(postgres_conn_id="gp_conn")
  conn = hook.get_conn()
  cur = conn.cursor()

  try:
    validate_filename(cur)
    
    # Обновим таблицу измерений для авто: если авто с таким car_id еще нет, добавим его
    update_dim_cars(cur)
    
    # Закроем записи которые больше не актуальны
    close_old_fact_cars(cur)
    
    # Добавление актуальных записей в таблицу фактов
    add_new_fact_cars(cur)
    
    # Если все запросы выполнились успешно, фиксируем изменения
    conn.commit()
    logging.info(f"Successfully inserted {len(cars)} records")
  except Exception as e:
    # Иначе откатываем их
    conn.rollback()
    logging.error(f"Error inserting data: {str(e)}")
    raise
  finally:
    # Закрываем подключение
    cur.close()
    conn.close()
```

##### Задание 2

> Так же, в docstrings, пометить элементы, несоответствующие основным принципам КХД. Переработать для соответствия.

В `Задание 1` уже в общих чертах было упомянуто что конкретно было исправлено для соответствия принципам КХД, так что здесь немного раскроем тему.

Для организации КХД была применена методология [Dimensional Modeling](https://en.wikipedia.org/wiki/Dimensional_modeling).
Историзм был реализован по принципу [Slowly Changing Dimensions](https://ru.wikipedia.org/wiki/Медленно_меняющееся_измерение) типа **SCD 2**.

Таблицы, [создаваемые](./init-gp/init.sql) по вышеописанному подходу:

```sql
CREATE TABLE IF NOT EXISTS dim_cars (
	car_sk serial PRIMARY KEY,
	mark text NOT NULL,
	model text NOT NULL,
	engine_volume text NOT NULL,
	year_of_manufacture integer NOT NULL,
	last_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_cars (
	sk serial PRIMARY KEY,
	car_sk integer NOT NULL,
	price_foreign numeric NOT NULL,
	currency_code_foreign text NOT NULL,
	price_rub numeric NOT NULL,
	ex_rate numeric NOT NULL,
	effective_from timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	effective_to timestamp NOT NULL DEFAULT 'infinity',
	is_current boolean NOT NULL DEFAULT true,
	source_filename text NOT NULL,
	last_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_car_sk FOREIGN KEY (car_sk) REFERENCES dim_cars(car_sk)
);
```

В таблице измерений будут храниться:
- Суррогатный ключ (id авто)
- Марка
- Модель
- Объем двигателя
- Год производства
- Дата модификации записи

В таблице фактов будут храниться:
- Суррогатный ключ
- Внешний ключ на таблицу измерений
- Код иностранной валюты
- Цена в иностранной валюте
- Курс обмена
- Цена в рублях
- Дата создания записи
- Дата закрытия записи (неактуальность)
- Флаг актуальности
- Имя файла из которого был взят этот кортеж
- Дата модификации записи

> [!NOTE]
> В целом, атрибут `is_current` избыточен, т.к. effective_to уже указывает на актуальность, но в аналитических запросах проверка is_current = true выглядит более явной чем effective_to = infinity.
> То же самое можно сказать про атрибуты `last_modified` в обеих таблицах, т.к. в таблице измерений изменения не предполагаются, а в таблице фактов этот атрибут будет равен или effective_from или effective_to.

##### Задание 3

> Спроектировать вьюху для аналитического отчета "Динамика цены автомобиля за месяц", с возможностью составить аналитику в разрезе марки или модели или года выпуска. Описать структуру в любой удобной форме.

Создание представления

```sql
CREATE OR REPLACE VIEW car_price_dynamics AS
WITH monthly_data AS (
	SELECT
		fc.car_sk as car_sk,
		price_rub,
		mark,
		model,
		year_of_manufacture,
		DATE_TRUNC('month', effective_from) AS month
	FROM fact_cars fc
	JOIN dim_cars dc ON fc.car_sk = dc.car_sk
)
SELECT
  mark,
  model,
  year_of_manufacture,
  month,
  AVG(price_rub) as avg_price,
	MIN(price_rub) as min_price,
	MAX(price_rub) as max_price,
	MAX(price_rub) - MIN(price_rub) as price_range
FROM monthly_data as md
GROUP BY car_sk, mark, model, year_of_manufacture, month
ORDER BY car_sk;
```

Использование представления

```sql
-- Просмотр аналитики цен по месяцам для всех авто
SELECT * FROM car_price_dynamics

-- В разрезе марки
SELECT * FROM car_price_dynamics
WHERE mark = 'Audi'

-- В разрезе модели
SELECT * FROM car_price_dynamics
WHERE model = 'A4'

-- В разрезе года выпуска
SELECT * FROM car_price_dynamics
WHERE year_of_manufacture = 2022

-- Или всё сразу
SELECT * FROM car_price_dynamics
WHERE 
	mark = 'Audi' 
	AND model = 'A4'
	AND year_of_manufacture = 2022
```

Представление будет содержать данные следующего вида

![](./docs/assets/Снимок%20экрана%202025-05-13%20090744.png)

## Локальная развертка

Предполагается развертка под [Docker Desktop](https://www.docker.com/)

[docker-compose.yml](./docker-compose.yaml)

### Запуск приложения

```bash
docker compose --profile flower up --remove-orphans
```

> [!NOTE]
> Если что-то очень пошло не так, можно всё сбросить и начать сначала
> ```bash
> docker compose --profile flower down --volumes --remove-orphans
> ```

### Начало работы

Зайдите в дэшборд Airflow по адресу `localhost:8080` и войдите с именем и паролем airflow/airflow.

Запустите DAG `refill_cars` для заполнения исходных данных в БД.

![](./docs/assets/Снимок%20экрана%202025-05-13%20091819.png)

Чтобы симулировать случайное изменение цен или добавление/удаление автомобилей используйте DAG `update_cars`.

![](./docs/assets/Снимок%20экрана%202025-05-13%20091944.png)

Чтобы после обновления добавить файл с актуальными машинами в S3, вызовите DAG `s3_save_current`.

![](./docs/assets/Снимок%20экрана%202025-05-13%20092146.png)

Вышеописанные даги являются прямой реализацией условия задачи:
> В бакет на S3 в будние дни по расписанию кладется файл csv с содержимым

Так что помимо ручного применения дагов они рассчитаны на автоматический вызов по соответствующему расписанию:

- `update_cars` вызывается по будням в 22:00 для симуляции изменения состояния автопарка
- `s3_save_current` вызывается по будням в 23:00 для доставки файла в S3
- `merge_cars` вызывается с понедельника по субботу в 00:00 для записи в КХД

Чтобы обновить КХД с новыми записями используйте `merge_cars`

![](./docs/assets/Снимок%20экрана%202025-05-13%20092540.png)

## Пример работы

Состояние таблицы fact_cars после нескольких выполнений DAG
![](./docs/assets/Снимок%20экрана%202025-05-13%20121124.png)
