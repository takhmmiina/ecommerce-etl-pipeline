import json
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
import clickhouse_connect

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from _lib.tenant import student_dag


LOGIN = os.path.basename(os.path.dirname(os.path.abspath(__file__)))

LIMIT = 100

SOURCE_URLS = {
    "users": f"https://dummyjson.com/users?limit={LIMIT}",
    "products": f"https://dummyjson.com/products?limit={LIMIT}",
    "carts": f"https://dummyjson.com/carts?limit={LIMIT}",
    "orders": f"https://dummyjson.com/carts?limit={LIMIT}",
}

SOURCE_ROOT_KEY = {
    "users": "users",
    "products": "products",
    "carts": "carts",
    "orders": "carts",
}

PG_CONN = dict(
    host="postgres",
    port=5432,
    dbname="aiacademy",
    user=LOGIN,
    password=os.environ.get("PG_PASSWORD", "zhumakova_pass123"),
)

CH_PASSWORD = os.environ.get("CH_PASSWORD", "2cbf3ab79547105399bc3b22")

CH_CONN = dict(
    host="clickhouse",
    port=8123,
    username="admin",
    password=CH_PASSWORD,
    database=LOGIN,
)


def ingest(source: str, **_):
    url = SOURCE_URLS[source]
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    items = resp.json()[SOURCE_ROOT_KEY[source]]

    table = f"raw_{source}"
    ingested_at = datetime.now(timezone.utc)

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(f"drop table if exists {table}")
            cur.execute(f"""
                create table {table} (
                    raw_json jsonb not null,
                    ingested_at timestamptz not null
                )
            """)
            psycopg2.extras.execute_values(
                cur,
                f"insert into {table} (raw_json, ingested_at) values %s",
                [(json.dumps(item), ingested_at) for item in items],
                template="(%s::jsonb, %s)",
            )
        conn.commit()


def load_to_clickhouse(source: str, **_):
    """РџРµСЂРµСЃРѕР·РґР°РµРј raw_<source> РІ ClickHouse Рё Р·Р°Р»РёРІР°РµРј РґР°РЅРЅС‹Рµ РёР· Postgres."""
    table = f"raw_{source}"

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(f"select raw_json, ingested_at from {table}")
            rows = cur.fetchall()

    client = clickhouse_connect.get_client(**CH_CONN)
    client.command(f"drop table if exists {table}")
    client.command(f"""
        create table {table} (
            raw_json String,
            ingested_at DateTime
        ) engine = MergeTree
        order by ingested_at
    """)

    if rows:
        now_utc = datetime.now(timezone.utc)
        data = [[json.dumps(raw_json), dt if dt is not None else now_utc] for raw_json, dt in rows]
        client.insert(table, data, column_names=["raw_json", "ingested_at"])


with student_dag(
    __file__,
    dag_id="dz_dbt_airflow_01",
    schedule=None,
    catchup=False,
) as dag:

    dbt_task = BashOperator(
        task_id="dbt_build",
        bash_command="""
            echo "Searching for dbt_project.yml..."
            FOUND_PATH=$(find /opt/airflow/dags/{login} -name "dbt_project.yml" -print -quit)
            
            if [ -z "$FOUND_PATH" ]; then
                echo "Error: dbt_project.yml not found!"
                exit 1
            fi

            ORIG_DBT_DIR=$(dirname "$FOUND_PATH")
            DBT_DIR="/tmp/dbt-orders"
            rm -rf "$DBT_DIR"
            mkdir -p "$DBT_DIR"
            cp -r "$ORIG_DBT_DIR"/* "$DBT_DIR"/

            find "$DBT_DIR" -type d -name ".ipynb_checkpoints" -exec rm -rf {{}} +
            find "$DBT_DIR" -name "*-checkpoint.*" -delete
            find "$DBT_DIR" -name "*.ipynb" -delete

            PROFILES_FILE="$DBT_DIR/profiles.yml"
            cat << 'EOF' > "$PROFILES_FILE"
{login}:
  outputs:
    clickhouse:
      type: clickhouse
      schema: {login}
      host: clickhouse
      port: 8123
      user: admin
      password: "{ch_password}"
      database: {login}
      driver: http
  target: clickhouse
EOF

            export DBT_PROFILES_DIR="$DBT_DIR"
            unset DBT_USER
            unset DBT_PASSWORD
            
            dbt clean --project-dir "$DBT_DIR" --profiles-dir "$DBT_PROFILES_DIR" --target clickhouse
            dbt build --project-dir "$DBT_DIR" --profiles-dir "$DBT_PROFILES_DIR" --target clickhouse
        """.format(login=LOGIN, ch_password=CH_PASSWORD),
        append_env=True,
    )

    for source in ("users", "products", "carts", "orders"):
        ingest_task = PythonOperator(
            task_id=f"ingest_{source}",
            python_callable=ingest,
            op_kwargs={"source": source},
        )
        load_task = PythonOperator(
            task_id=f"load_{source}_to_clickhouse",
            python_callable=load_to_clickhouse,
            op_kwargs={"source": source},
        )
        ingest_task >> load_task >> dbt_task 
