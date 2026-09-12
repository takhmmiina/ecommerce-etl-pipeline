from datetime import datetime, timedelta
import logging
import json
import urllib.request
import urllib.parse
import psycopg2
from psycopg2.extras import RealDictCursor

from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from _lib.tenant import student_dag

logger = logging.getLogger("airflow.task")

LOGIN = "zhumakova"

PG_PARAMS = {
    "host": "postgres",
    "port": 5432,
    "user": LOGIN,
    "password": f"{LOGIN}_pass123",
    "dbname": "aiacademy",
}

CH_HOST = "http://clickhouse:8123"
CH_USER = "admin"
CH_PASS = "2cbf3ab79547105399bc3b22"

default_args = {
    'owner': LOGIN,
    'start_date': datetime(2026, 8, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with student_dag(
    __file__,
    dag_id="pg_to_clickhouse",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    params={
        "limit": Param(30, type="integer", description="РљРѕР»РёС‡РµСЃС‚РІРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№")
    },
    tags=['homework'],
) as dag:

    check_source = BashOperator(
        task_id='check_source',
        bash_command='echo "Starting batch pipeline API -> PostgreSQL -> ClickHouse"'
    )

    @task
    def ingest_api_to_postgres(**context):
        limit = context.get('params', {}).get('limit', 30)
        api_url = Variable.get("api_url_users", default_var="https://dummyjson.com/users")
        url = f"{api_url}?limit={limit}"
        
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            users = data.get('users', [])

        conn = psycopg2.connect(**PG_PARAMS)
        cursor = conn.cursor()
        
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {LOGIN}.ods_raw_users (
                id INT,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                age INT,
                gender VARCHAR(20),
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # РЈР±СЂР°РЅ TRUNCATE, С‡С‚РѕР±С‹ РґР°РЅРЅС‹Рµ РІ raw-СЃР»РѕРµ РЅР°РєР°РїР»РёРІР°Р»РёСЃСЊ СЃ РєР°Р¶РґС‹Рј Р·Р°РїСѓСЃРєРѕРј

        for u in users:
            cursor.execute(f"""
                INSERT INTO {LOGIN}.ods_raw_users (id, first_name, last_name, email, age, gender)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (u.get('id'), u.get('firstName'), u.get('lastName'), u.get('email'), u.get('age'), u.get('gender')))
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Successfully inserted {len(users)} users into Postgres ODS accumulatively.")
        return len(users)

    @task
    def transform_postgres_data(ingested_count: int):
        logger.info(f"ingested_count from previous task = {ingested_count}")
        conn = psycopg2.connect(**PG_PARAMS)
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS {LOGIN}.stg_unique_users;")
        cursor.execute(f"""
            CREATE TABLE {LOGIN}.stg_unique_users AS
            SELECT DISTINCT ON (id) id, first_name, last_name, email, age, gender, ingested_at
            FROM {LOGIN}.ods_raw_users
            ORDER BY id, ingested_at DESC;
        """)

        conn.commit()
        cursor.close()
        conn.close()
        return ingested_count

    @task
    def load_to_clickhouse(unique_count: int):
        logger.info(f"unique_count received = {unique_count}")
        
        pg_conn = psycopg2.connect(**PG_PARAMS)
        pg_cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
        pg_cursor.execute(f"SELECT id, first_name, last_name, email, age, gender FROM {LOGIN}.stg_unique_users;")
        rows = pg_cursor.fetchall()
        pg_cursor.close()
        pg_conn.close()

        logger.info(f"rows fetched from Postgres for ClickHouse = {len(rows)}")

        def execute_ch(payload: bytes):
            params = {
                'user': CH_USER,
                'password': CH_PASS,
            }
            url = f"{CH_HOST}/?{urllib.parse.urlencode(params)}"
            headers = {'Content-Type': 'text/plain; charset=utf-8'}
            req = urllib.request.Request(url, data=payload, headers=headers)
            try:
                with urllib.request.urlopen(req) as resp:
                    return resp.read().decode('utf-8')
            except urllib.error.HTTPError as e:
                err_text = e.read().decode('utf-8')
                logger.error(f"ClickHouse Error: {err_text}")
                raise e

        execute_ch(f"CREATE DATABASE IF NOT EXISTS {LOGIN};".encode('utf-8'))
        execute_ch(f"DROP TABLE IF EXISTS {LOGIN}.fact_users;".encode('utf-8'))
        execute_ch(f"""
            CREATE TABLE {LOGIN}.fact_users (
                id UInt32,
                first_name String,
                last_name String,
                email String,
                age UInt8,
                gender String
            ) ENGINE = MergeTree() ORDER BY id;
        """.encode('utf-8'))

        if not rows:
            logger.warning("No rows to insert into ClickHouse!")
            return

        values_list = []
        for r in rows:
            uid = int(r['id'])
            fn = str(r['first_name'] or "").replace("'", "\\'")
            ln = str(r['last_name'] or "").replace("'", "\\'")
            em = str(r['email'] or "").replace("'", "\\'")
            age = int(r['age'] or 0)
            gen = str(r['gender'] or "").replace("'", "\\'")
            
            values_list.append(f"({uid}, '{fn}', '{ln}', '{em}', {age}, '{gen}')")
        
        raw_body = f"INSERT INTO {LOGIN}.fact_users VALUES " + ", ".join(values_list)
        execute_ch(raw_body.encode('utf-8'))
        logger.info("Data successfully inserted into ClickHouse!")

    ingest_task = ingest_api_to_postgres()
    transform_task = transform_postgres_data(ingest_task)
    load_task = load_to_clickhouse(transform_task)

    check_source >> ingest_task >> transform_task >> load_task
