# End-to-End ETL Pipeline & DWH for E-Commerce Analytics

An automated data pipeline that ingests, transforms, and loads e-commerce data into a columnar data warehouse for analytical reporting.

## Architecture & Tech Stack
- **Orchestration:** Apache Airflow
- **Transformation & Modeling:** dbt (data build tool)
- **Data Warehouse:** ClickHouse
- **Operational Database:** PostgreSQL
- **Containerization:** Docker & Docker Compose
- **Language:** Python, SQL

## Pipeline Workflow
1. **Ingestion:** Data is extracted from source operational tables in PostgreSQL.
2. **Orchestration:** Apache Airflow manages and schedules daily DAG execution.
3. **Transformation:** dbt applies staging, intermediate, and mart models, performing data cleansing, testing, and aggregations.
4. **Loading:** Transformed analytical datasets are loaded into ClickHouse for high-performance BI reporting.

## Project Structure
```text
├── airflow/
│   ├── dags/             # Airflow DAG definitions
│   └── Dockerfile        # Custom Airflow image setup
├── dbt_project/
│   ├── models/           # dbt models (staging, marts)
│   ├── tests/            # Data quality and integrity tests
│   └── dbt_project.yml   # Project configuration
├── docker-compose.yml    # Services orchestration (Airflow, ClickHouse, Postgres)
└── README.md
