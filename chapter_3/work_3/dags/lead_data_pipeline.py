import csv
import random
import logging
import uuid
import polars as pl

from faker import Faker
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Configure logging.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)


def _create_data(locale: str) -> Faker:
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)


def _generate_record(fake: Faker) -> list:
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = ''.join(filter(str.isdigit, fake.ssn()))
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ")
    phone_number = f"+52 1 {random.randint(55, 56)}{random.randint(1000,9999)}{random.randint(1000,9999)}"
    mac_address = fake.mac_address()
    ip_address = fake.ipv4()
    iban = fake.iban()
    accessed_at = fake.date_time_between("-1y")
    session_duration = random.randint(0, 36000)
    download_speed = random.randint(0, 1000)
    upload_speed = random.randint(0, 800)
    consumed_traffic = random.randint(0, 2000000)

    return [
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic
    ]


def _write_to_csv() -> None:
    fake = _create_data("es_MX")
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    if str(date.today()) == "2024-09-23":
        rows = random.randint(100_372, 100_372)
    else:
        rows = random.randint(0, 1101)

    with open("/opt/airflow/data/raw_data.csv", mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for _ in range(rows):
            writer.writerow(_generate_record(fake))

    logging.info(f"Written {rows} records to the CSV file.")


def _add_id() -> None:
    df = pl.read_csv(
        "/opt/airflow/data/raw_data.csv",
        dtypes={
            "personal_number": pl.Utf8,
            "session_duration": pl.Int64,
            "download_speed": pl.Int64,
            "upload_speed": pl.Int64,
            "consumed_traffic": pl.Int64,
        }
    )
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    df.write_csv("/opt/airflow/data/raw_data.csv")
    logging.info("Added UUID to the dataset.")


def _update_datetime() -> None:
    if str(date.today()) != "2024-09-23":
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        df = pl.read_csv(
            "/opt/airflow/data/raw_data.csv",
            dtypes={"accessed_at": pl.Utf8}
        )
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        df.write_csv("/opt/airflow/data/raw_data.csv")
        logging.info("Updated accessed timestamp.")


def save_raw_data():
    logging.info(f"Started batch processing for {date.today()}.")
    _write_to_csv()
    _add_id()
    _update_datetime()
    logging.info(f"Finished batch processing {date.today()}.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'lead_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline.',
    schedule_interval="0 7 * * *",  # Corre a las 7:00 AM UTC
    start_date=datetime(2024, 9, 22),
    catchup=False,
)

extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)

create_raw_schema_task = SQLExecuteQueryOperator(
    task_id='create_raw_schema',
    conn_id='postgres_conn',
    sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag=dag,
)

create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
            person_name VARCHAR(100),
            user_name VARCHAR(100),
            email VARCHAR(100),
            personal_number NUMERIC, 
            birth_date VARCHAR(100), 
            address VARCHAR(150),
            phone VARCHAR(100), 
            mac_address VARCHAR(100),
            ip_address VARCHAR(100),
            iban VARCHAR(100),
            accessed_at TIMESTAMP,
            session_duration INT,
            download_speed INT,
            upload_speed INT,
            consumed_traffic INT,
            unique_id VARCHAR(100)
        );
    """,
    dag=dag,
)

load_raw_data_task = SQLExecuteQueryOperator(
    task_id='load_raw_data',
    conn_id='postgres_conn',
    sql="""
    COPY driven_raw.raw_batch_data(
        person_name, user_name, email, personal_number, birth_date,
        address, phone, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic, unique_id
    ) 
    FROM '/opt/airflow/data/raw_data.csv' 
    DELIMITER ',' 
    CSV HEADER;
    """,
    dag=dag,
)

run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
    dag=dag,
)

run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
    dag=dag,
)

[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task
