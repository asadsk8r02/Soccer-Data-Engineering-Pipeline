from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .config("spark.jars", "/Drivers/SQL_Server/jdbc/postgresql-42.7.3.jar") \
        .getOrCreate()
    return spark

def table_exists(spark, jdbc_url, connection_properties, table_name):
    """Check if a table exists in the PostgreSQL database."""
    query = f"(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}') AS t"
    try:
        df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
        return df.count() > 0
    except Exception:
        return False

def read_and_write_csv_to_postgres(spark, csv_path, db_table, jdbc_url, connection_properties):
    """Read a CSV file into a DataFrame and write it to PostgreSQL."""
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Determine write mode based on whether the table exists
    if table_exists(spark, jdbc_url, connection_properties, db_table):
        write_mode = "overwrite"
    else:
        write_mode = "append"

    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", db_table) \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .mode(write_mode) \
        .save()

def main():
    spark = create_spark_session()

    jdbc_url = "jdbc:postgresql://postgres:5432/arsenalfc"
    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Define CSV file paths and corresponding database tables
    data_files = {
        "/tmp/data/matches.csv": "arsenalmatches",
        "/tmp/data/goalkeepers.csv": "arsenalgk",
        "/tmp/data/players.csv": "arsenalplayers"
    }

    for csv_path, db_table in data_files.items():
        read_and_write_csv_to_postgres(spark, csv_path, db_table, jdbc_url, connection_properties)

if __name__ == "__main__":
    main()
