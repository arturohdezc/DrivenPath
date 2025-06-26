import random
import csv
import logging
import uuid
import polars as pl
from faker import Faker
from datetime import date, datetime, timedelta


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)

def create_data(locale: str) -> Faker:
    logging.info(f"Creando datos sintéticos para el país: {locale.split('_')[-1]}")
    return Faker(locale)

def generate_record(fake: Faker) -> list:
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = fake.ssn()
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ")
    phone_number = fake.phone_number()
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
        address, phone_number, mac_address, ip_address, iban,
        accessed_at, session_duration, download_speed, upload_speed, consumed_traffic
    ]

def write_to_csv(file_path: str, rows: int) -> None:
    fake = create_data("es_MX")  
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date",
        "address", "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]
    with open(file_path, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for _ in range(rows):
            writer.writerow(generate_record(fake))
    logging.info(f"{rows} registros fueron escritos en {file_path}")

def add_id(file_name) -> None:
    df = pl.read_csv(file_name)
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    df.write_csv(file_name)
    logging.info("UUIDs agregados al dataset.")

def update_datetime(file_name: str, run: str) -> None:
    if run == "next":
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        df = pl.read_csv(file_name)
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        df.write_csv(file_name)
        logging.info("Timestamp actualizado a ayer.")

if __name__ == "__main__":
    logging.info(f"Iniciando procesamiento batch para {date.today()}")
    output_file = f"chapter_2/src_2/data_2/batch_{date.today()}.csv"
    if str(date.today()) == "2024-09-14":  
        records = 100_372
        run_type = "first"
    else:
        records = random.randint(0, 1000)
        run_type = "next"

    write_to_csv(output_file, records)
    add_id(output_file)
    update_datetime(output_file, run_type)
    logging.info(f"Proceso batch finalizado para {date.today()}")
