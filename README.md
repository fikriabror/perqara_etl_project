# ETL Data Processing with PostgreSQL

## Project Overview

This project implements an Extract, Transform, Load (ETL) pipeline using Apache Airflow and PostgreSQL on top of Docker.
specifically focusing on Extract, Transform, loading data into a PostgreSQL.

## Prerequisites

Prerequisites
Install Python, Docker, and PostgreSQL on your local machine.

## Dags
etl_dag.py contains all end to end ETL process until the data load into database posgresql

## Data 
Data contains all the input csv file that will be loaded into postgresql and extract to transform new data model to data mart

## Scripts
to run the docker you can execute run_airflow.sh

Initialize run database Init
docker-compose run --rm webserver airflow db init

Docker Compose
docker-compose up -d

Access Airflow:
Navigate to http://localhost:8080 in your web browser (default host & port).

Login to Airflow:
Use the default credentials:

Username: admin
Password: admin

Set Up PostgreSQL Connection in Airflow (This is for my postgres in my local machine):

Create a new connection with the following details:

Connection ID: local_postgres
Host: host.docker.internal
Schema: postgres
Password: postgres
Port: 5432
login: postgres

If you have any problem with the login:
Reset the Airflow Password
If you suspect an issue with the credentials, you can reset the Airflow admin password. Follow these steps:

Access the Airflow Webserver Container: docker-compose exec webserver /bin/bash

airflow users create
--username admin
--firstname Admin
--lastname User
--email admin@example.com
--role Admin
--password newpassword

Run the DAG:

Go to the home page in Airflow and trigger the etl_dag to start the ETL process.

Notes The postgres setup it's working for my local machine. Need to be adjusted based on your local machine

If you have any questions or need further assistance, please feel free to reach out.

## Sample Data

I have provided the data inside the `data` folder within the project directory with some modifications to support the transformation.

## How to Run
- First you have to import data in your postgres database local machine,
- Run the docker-compose up --build
- Visit your http://localhost:8080/
- Next step need to add another database and registered the db in the airflow connection called local_postgres
- Run the DAG tasks
