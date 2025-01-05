# ETL Project with Airflow

This project implements an ETL pipeline using Apache Airflow to extract, transform, and load data. It includes scripts to initialize a database schema, run DAGs for historical backfills, and populate the database daily.

## Project Structure

```
├── airflow_files
│    │   └──(Airflow DAGs for the project)
│    ├── dags
│    ├── tasks
│    │    ├── data_extraction
│    │    ├── data_load
│    │    └── data_transformation
│    └── temp_data
│          └──(Temporary CSV files used during processing)
├── database
│    └── init_schema
│          └──(Scripts to initialize the database schema)
└── docker-compose.yml
    └──(Docker Compose file to define the container setup)

```

## Prerequisites

- **Docker**: Ensure Docker is installed on your machine. You can download it from [Docker's official site](https://www.docker.com/).

## Setup Instructions

### 1. Compose Docker

Start the Docker container by running:

```bash
docker-compose up -d
```

### 2. Initialize Airflow Database

Initialize the Airflow database inside the webserver container:

```bash
docker exec -it <container-name-webserver> airflow db init
```

### 3. Create Airflow User

Create an admin user for Airflow:

docker exec -it etl_and_data_analysis-airflow-webserver-1 bash

```bash
airflow users create \
    --username USERNAME \
    --firstname FIRSTNAME \
    --lastname LASTNAME \
    --email EMAIL \
    --role Admin
```

Replace `USERNAME`, `FIRSTNAME`, `LASTNAME`, and `EMAIL` with your desired credentials.

### 4. Access Airflow Webserver

- Open your browser and navigate to http://localhost:8080.
- Log in using the credentials created in the previous step.

### 5. Run DAGs

- Run the Backfill DAGs:
  - dag_backpopulate_product_table: Populates the product table with historical data.
  - dag_backpopulate_past_orders: Loads past order data into the database.
- Enable the Daily DAG:
  Turn on dag_daily_orders to populate the database with daily updates.

## Notes

- The airflow_files/temp_data folder is used for temporary storage of CSV files during processing. Ensure sufficient disk space is available.
- The database/init_schema script automatically creates the necessary schema when the Docker container is started.

## Troubleshooting

If you encounter issues accessing the webserver or running commands, ensure the Docker containers are running using:

```bash
docker ps
```

Check Airflow logs for debugging:

```bash
docker logs <container-name-webserver>
docker logs <container-name-scheduler>
docker logs <container-name-postgres>
```

Enjoy your ETL pipeline!
