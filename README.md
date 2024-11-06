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

<pre><code>
cd /path/to/Docker_files
docker-compose up -d
</code></pre>

The containers should look like this: 
<img width="1249" alt="image" src="https://github.com/user-attachments/assets/50f2a08b-b026-4eaa-a397-f6b967aead10">

Before proceeding please check:
<img width="1249" alt="image" src="https://github.com/user-attachments/assets/d7b6696e-6335-4853-8cad-874c8adf904d">

Access Jupyter at http://localhost:8085/.
You can run the jupyter notebooks to test and verify the table creation in Postgresql.
After running the notebooks check the postgres container again to verify.

Execute ETL pipeline by accessing the Airflow UI, which will load data into PostgreSQL within the arsenalfc database (DWH schemas).
To access the Airflow UI:
<pre><code> 
http://localhost:8090/
</code> </pre>

You will soccerdag DAG in the Apache Airflow UI. Run it and check logs for any error.

Connect PostgreSQL to Tableau:
Open Tableau and select PostgreSQL as the data source.

## Data Extraction, Transformation, & Loading (ETL)

Using Apache Spark’s JDBC connector, raw data is pulled from PostgreSQL. This data undergoes cleaning, standardization, and transformations, including deduplication, calculation of derived metrics, and table joins. The processed data is then loaded into a structured data warehouse schema, optimized for analytical queries.

## Analysis & Visualization with Tableau & Jupyter Notebooks

Tableau dashboards offer interactive visualizations covering player statistics, match results, and seasonal performance trends. For ad-hoc analysis, Jupyter Notebooks are also utilized.

## Orchestration with Apache Airflow

Apache Airflow coordinates the ETL tasks, automating each step and providing a clear workflow visualization for easy monitoring. The ETL process is managed within the etl_arsenalfc DAG, scheduled to ensure data is consistently updated and ready for analysis.


## Results

<img width="986" alt="Screenshot 2024-11-05 at 5 54 53 PM" src="https://github.com/user-attachments/assets/d638754c-2e03-4423-87da-fe28707bf3ab">


<img width="1011" alt="Screenshot 2024-11-05 at 6 47 26 PM" src="https://github.com/user-attachments/assets/accc5312-bd09-4a93-8e1b-e93009f370c4">

