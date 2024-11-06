# Soccer ETL Data Pipeline

This project establishes a robust ETL data engineering pipeline designed to gather, transform, and examine data related to Soccer Club’s players, goalkeepers, and match records spanning from 2017 to 2023. It generates valuable insights into individual player statistics, game results, and goalkeeper performance trends.

## Tech stack & Tools

- **Infrastructure**: Docker
- **Data Warehouse**: PostgreSQL
- **Database**: PostgreSQL
- **Orchestration**: Apache Airflow
- **Data Processing**: Apache Spark
- **ETL development/testing scripts**: Jupyter Notebook (Notebooks << .ipynb files)
- **ETL productions scripts**: .py files (ETL Scripts << .py files)
- **Serving Layer**: Tableau

## Pipeline Overview

The pipeline begins by importing data from CSV files using Apache Spark. Following the ETL (Extract, Transform, Load) steps, the data is stored in PostgreSQL as a central repository. Apache Airflow oversees the scheduling and execution of these tasks, enabling streamlined and automated workflows. Finally, Tableau is used for data visualization, transforming results into dynamic dashboards and detailed reports for insightful analysis.

## Getting Started

Follow these steps to set up the project locally for development and testing.

## Prerequisites

Docker and Docker Compose

## Installation

Clone the repository.
Navigate to the Docker_files directory.

## Build the infrastructure:

<pre> <code> cd /path/to/docker_folder
docker-compose up -d</code> </pre>

Access Jupyter at http://localhost:8085/.

Execute ETL tasks by running the Jupyter notebooks, which will load data into PostgreSQL within the arsenalfc database (DWH schemas).

Connect PostgreSQL to Tableau:
Open Tableau and select PostgreSQL as the data source.

Enter the following details:

<pre> <code> server: localhost:5442
database: arsenalfc</code> </pre>

## Data Extraction, Transformation, & Loading (ETL)

Using Apache Spark’s JDBC connector, raw data is pulled from PostgreSQL. This data undergoes cleaning, standardization, and transformations, including deduplication, calculation of derived metrics, and table joins. The processed data is then loaded into a structured data warehouse schema, optimized for analytical queries.

## Analysis & Visualization with Tableau & Jupyter Notebooks

Tableau dashboards offer interactive visualizations covering player statistics, match results, and seasonal performance trends. For ad-hoc analysis, Jupyter Notebooks are also utilized. The 8 Tableau dashboards include:

## Orchestration with Apache Airflow

Apache Airflow coordinates the ETL tasks, automating each step and providing a clear workflow visualization for easy monitoring. The ETL process is managed within the etl_arsenalfc DAG, scheduled to ensure data is consistently updated and ready for analysis.

Access the Airflow UI:

<pre> <code> 
http://localhost:8090/dags/etl_arsenalfc/</code> </pre>
