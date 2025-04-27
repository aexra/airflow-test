# Airflow Local Tests

## Запуск чистого контейнера с Airflow

В папку с проектом скачиваем docker-compose.yaml
```bash
curl -LfO "https://airflow.apache.org/docs/apache-airflow/2.10.4/docker-compose.yaml"
```

Создаем конфиг чтобы Airflow не ругался:

```bash
# Windows
powershell -command "echo 'AIRFLOW_UID=50000' | Out-File -Encoding utf8 .env"
```
```bash
# Linux
echo "AIRFLOW_UID=50000" | iconv -t utf-8 > .env
```

Инициализируем конфиг

```bash
docker compose run airflow-cli airflow config list
```

Инициализируем Airflow

```bash
docker compose up airflow-init
```

Собираем весь контейнер

```bash
docker compose --profile flower up --remove-orphans
```

## Сброс контейнера если что-то пошло не так

```bash
docker compose --profile flower down --volumes --remove-orphans
```

Или радикально:

```bash
docker compose --profile flower down --volumes --remove-orphans --rmi all
```