# The Optimal Municipality in Switzerland

## Contributors: Kevin Stadelmann, Marc Weber, Patricia Wellhöfer


### Project Description / Purpose
This projects is an assignment of HSLU MSc in Applied Information and Data Science module Data Warehousing and Data Lake Systems 1. It includes work in the context of the 
technologies Apache Airflow and Amazon Web Services (AWS). Topic is about finding the optimal municipality in Switzerland. Different data sources are collected for this purpose.


### Structure
airflow_dag: Python file for Apache Airflow setup

database_management: Database Management classes for connection and log (Python)

docker configuration: Docker configuration file

extraction: Methods for extraction (Python)

load: Methods for loading of data (Python)

sql_scripts: Database scripts for setup of job and log tables (PostgreSQL)

requirements.txt: Required Python packages on Apache Airflow environment


### How to install
Following steps are needed to set up the data pipelines:
1.	Set up Apache Airflow with Docker (including install required packages -> requirements.txt)
2.	Add all needed connection information in Apache Airflow
	- AWS S3 connection
	- AWS RDS Postgres connection
3.	Deploy Python files in Airflow DAG folder and on database (sql_scripts)
4.	Configure all jobs on the database (table job.t_job)


