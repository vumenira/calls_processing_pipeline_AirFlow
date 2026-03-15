from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pymysql
import duckdb
import json
import os

MYSQL_CONFIG = {
    "host": "host.docker.internal",
    "user": "root",
    "password": "MySQL_Student123",
    "database": "calls_db",
    "port": 3306
}

API_PATH = "/usr/local/airflow/data/calls_json"

DUCKDB_PATH = "/usr/local/airflow/data/calls.duckdb"



def detect_new_calls(**context):

    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    last_loaded_call_time = Variable.get("last_loaded_call_time", default = '2000-01-01 00:00:00')
    cursor.execute("""
    select call_id 
    from calls
    where call_time >= %s""", (last_loaded_call_time,))
 
    new_calls = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="new_calls", value=new_calls)

    print(f"{len(new_calls)} new calls detected.")



def load_telephony_from_api(**context):
    
    new_calls = context["ti"].xcom_pull(task_ids = "detect_new_calls", key = "new_calls")

    if not new_calls:
        print("No calls detected.")
        return
    
    telephony = {}

    for filename in os.listdir(API_PATH):
        if filename.endswith(".json"):
            with open(f"{API_PATH}/{filename}", "r") as data:
                records = json.load(data)
            for record in records:
                telephony[record["call_id"]] = {
                    "duration_sec": record["duration_sec"],
                    "short_description": record["short_description"]}

    for key in list(telephony.keys()):
        if key not in new_calls:
            telephony.pop(key)

    context["ti"].xcom_push(key = "telephony_data", value = telephony)
    print(f"{len(telephony)} telephony calls loaded.")



def validate_data(**context):

    new_calls = context["ti"].xcom_pull(task_ids="detect_new_calls", key="new_calls")
    telephony = context["ti"].xcom_pull(task_ids="load_telephony_from_api", key="telephony_data")
    errors = set()

    if not new_calls:
        print("No data to validate.")
        return

    if len(new_calls) != len(set(new_calls)):
        duplicates = [int(x) for x in new_calls if new_calls.count(x) > 1]
        errors.update(duplicates)
        print(f"Duplicate call_ids found: {set(duplicates)}")

    for call_id, data in telephony.items():
        if data["duration_sec"] is not None and data["duration_sec"] < 0:
            errors.add(int(call_id))
            print(f"call_id {call_id} has negative duration: {data['duration_sec']}")

    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    placeholders = ",".join(["%s"] * len(new_calls))
    cursor.execute(f"""
        SELECT c.call_id
        FROM calls c
        LEFT JOIN employees e ON c.employee_id = e.employee_id
        WHERE c.call_id IN ({placeholders})
        AND e.employee_id IS NULL
    """, tuple(new_calls))

    no_enployee = [int(row[0]) for row in cursor.fetchall()]
    if no_enployee:
        errors.update(no_enployee)
        print(f"call_ids with missing employee: {no_enployee}")

    cursor.close()
    conn.close()

    valid = [int(c) for c in new_calls if c not in errors]

    for call in errors:
        print(f"[QUALITY ERROR] {call}")

    print(f"[QUALITY OK] All checks passed for {len(valid)} calls.")

    context["ti"].xcom_push(key = "valid_calls", value = valid)



def transform_and_load_duckdb(**context):

    new_calls = context["ti"].xcom_pull(task_ids = "validate_data", key = "valid_calls")
    telephony = context["ti"].xcom_pull(task_ids = "load_telephony_from_api", key = "telephony_data")

    if not new_calls:
        print("Nothing to load, data is up to date.")
        return

    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    format_ids = ",".join(["%s"] * len(new_calls))
    cursor.execute(f"""
        SELECT
            c.call_id,
            c.employee_id,
            c.call_time,
            c.phone,
            c.direction,
            c.status,
            e.full_name,
            e.team,
            e.role,
            e.hire_date
        FROM calls c
        JOIN employees e ON c.employee_id = e.employee_id
        WHERE c.call_id IN ({format_ids})
    """, tuple(new_calls))
    
    mysql_data = cursor.fetchall()

    cursor.close()
    conn.close()

    merged_data = []
    for row in mysql_data:
        call_id, employee_id, call_time, phone, direction, status, full_name, team, role, hire_date = row
        merged_data.append((call_id, employee_id, call_time, phone, 
                            direction, status, full_name, team, role, 
                            hire_date, telephony.get(str(call_id), {}).get("duration_sec"),
                            telephony.get(str(call_id), {}).get("short_description")))

    duck = duckdb.connect(DUCKDB_PATH)
    duck.execute("""create table if not exists calls_big_table (
            call_id INT PRIMARY KEY,
            employee_id INT,
            call_time TIMESTAMP,
            phone VARCHAR,
            direction VARCHAR,
            status VARCHAR,
            full_name VARCHAR,
            team VARCHAR,
            role VARCHAR,
            hire_date DATE,
            duration_sec INT,
            short_description VARCHAR);""")
    
    duck.executemany("""insert or ignore into calls_big_table values (?,?,?,?,?,?,?,?,?,?,?,?);""", merged_data)

    duck.close()

    print(f"inserted {len(merged_data)} rows into DuckDB.")

    Variable.set("last_loaded_call_time", str(max(row[2] for row in mysql_data)))



default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "catchup": False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),}

with DAG(
    "calls_processing_dag",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,) as dag:

    t1 = PythonOperator(
        task_id="detect_new_calls",
        python_callable=detect_new_calls)
    t2 = PythonOperator(
        task_id="load_telephony_from_api",
        python_callable=load_telephony_from_api)
    t3 = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data)
    t4 = PythonOperator(
        task_id="transform_and_load_duckdb",
        python_callable=transform_and_load_duckdb)

    t1 >> t2 >> t3 >> t4