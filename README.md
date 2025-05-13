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
    2. Текущая реализация в принципе предполагает наличия только одного файла в бакете, что не позволит просматривать старые записи.
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



##### Задание 2

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

Чтобы обновить КХД с новыми записями используйте `merge_cars`

![](./docs/assets/Снимок%20экрана%202025-05-13%20092540.png)

Этот даг является прямой реализацией условия задачи:
> В бакет на S3 в будние дни по расписанию кладется файл csv с содержимым

Так что помимо ручного применения дагов они рассчитаны на автоматический вызов по соответствующему расписанию:

`update_cars` вызывается по будням в 22:00 для симуляции изменения состояния автопарка
`s3_save_current` вызывается по будням в 23:00 для доставки файла в S3
`merge_cars` вызывается с понедельника по субботу в 00:00 для записи в КХД