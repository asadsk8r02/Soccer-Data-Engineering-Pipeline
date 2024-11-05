from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder.config("spark.jars", "/Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar").getOrCreate()
    return spark

def read_and_write_csv_to_postgres(spark, csv_path, db_table):
    df = spark.read.csv(csv_path, header=True)
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", db_table) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

def main():
    spark = create_spark_session()

    # Paths to the CSV files
    matches_csv_path = "/tmp/data/matches.csv"
    goalkeepers_csv_path = "/tmp/data/goalkeepers.csv"
    players_csv_path = "/tmp/data/players.csv"

    # Database table names
    matches_db_table = "arsenalmatches"
    goalkeepers_db_table = "arsenalgk"
    players_db_table = "arsenalplayers"

    # Process each CSV file
    read_and_write_csv_to_postgres(spark, matches_csv_path, matches_db_table)
    read_and_write_csv_to_postgres(spark, goalkeepers_csv_path, goalkeepers_db_table)
    read_and_write_csv_to_postgres(spark, players_csv_path, players_db_table)

if __name__ == "__main__":
    main()


# from pyspark.sql import SparkSession

# def create_spark_session():
#     """Create and return a Spark session."""
#     spark = SparkSession.builder.config("spark.jars", "/Drivers/SQL_Server/jdbc/postgresql-42.7.3.jar").getOrCreate()
#     return spark

# def table_exists(spark, jdbc_url, connection_properties, table_name):
#     """Check if a table exists in the PostgreSQL database."""
#     try:
#         spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#         return True
#     except Exception as e:
#         return False

# def read_and_write_csv_to_postgres(spark, csv_path, db_table):
#     df = spark.read.csv(csv_path, header=True, inferSchema=True)

#     # JDBC connection properties
#     jdbc_url = "jdbc:postgresql://postgres:5432/arsenalfc"
#     connection_properties = {
#         "user": "postgres",
#         "password": "postgres",
#         "driver": "org.postgresql.Driver"
#     }

#     if table_exists(spark, jdbc_url, connection_properties, db_table):
#         # Table exists, use overwrite mode
#         write_mode = "overwrite"
#     else:
#         # Table does not exist, use append mode to create it
#         write_mode = "append"

#     df.write.format("jdbc") \
#         .option("url", jdbc_url) \
#         .option("dbtable", db_table) \
#         .option("user", connection_properties["user"]) \
#         .option("password", connection_properties["password"]) \
#         .option("driver", connection_properties["driver"]) \
#         .mode(write_mode) \
#         .save()

# def main():
#     spark = create_spark_session()

#     # Paths to the CSV files
#     matches_csv_path = "/tmp/data/matches.csv"
#     goalkeepers_csv_path = "/tmp/data/goalkeepers.csv"
#     players_csv_path = "/tmp/data/players.csv"

#     # Database table names
#     matches_db_table = "arsenalmatches"
#     goalkeepers_db_table = "arsenalgk"
#     players_db_table = "arsenalplayers"

#     # Process each CSV file
#     read_and_write_csv_to_postgres(spark, matches_csv_path, matches_db_table)
#     read_and_write_csv_to_postgres(spark, goalkeepers_csv_path, goalkeepers_db_table)
#     read_and_write_csv_to_postgres(spark, players_csv_path, players_db_table)

# if __name__ == "__main__":
#     main()


# from pyspark.sql import SparkSession
# from pyspark.sql.types import (
#     StructType, StructField, IntegerType, StringType, DateType, FloatType
# )
# from pyspark.sql.functions import to_date

# def create_spark_session():
#     """Create and return a Spark session."""
#     spark = SparkSession.builder.config("spark.jars", "/Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar").getOrCreate()
#     return spark

# def read_and_write_csv_to_postgres(spark, csv_path, db_table, schema, column_types, date_columns=[]):
#     df = spark.read.csv(csv_path, header=True, schema=schema)

#     # Parse date columns if necessary
#     for date_col, date_format in date_columns:
#         df = df.withColumn(date_col, to_date(df[date_col], date_format))

#     df.write.format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
#         .option("driver", "org.postgresql.Driver") \
#         .option("dbtable", db_table) \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .option("createTableColumnTypes", column_types) \
#         .mode("overwrite") \
#         .save()

# def main():
#     spark = create_spark_session()

#     # Paths to the CSV files
#     matches_csv_path = "/tmp/data/matches.csv"
#     goalkeepers_csv_path = "/tmp/data/goalkeepers.csv"
#     players_csv_path = "/tmp/data/players.csv"

#     # Database table names
#     matches_db_table = "arsenalmatches"
#     goalkeepers_db_table = "arsenalgk"
#     players_db_table = "arsenalplayers"

#     # Define schemas and column types for each CSV

#     # 1. Schema and column types for matches.csv
#     matches_schema = StructType([
#         StructField("Season", StringType(), True),
#         StructField("Tour", IntegerType(), True),
#         StructField("Date", StringType(), True),
#         StructField("Time", StringType(), True),
#         StructField("Opponent", StringType(), True),
#         StructField("HoAw", StringType(), True),
#         StructField("ArsenalScore", IntegerType(), True),
#         StructField("OpponentScore", IntegerType(), True),
#         StructField("Stadium", StringType(), True),
#         StructField("Attendance", IntegerType(), True),
#         StructField("Coach", StringType(), True),
#         StructField("Referee", StringType(), True)
#     ])

#     matches_column_types = """
#         Season VARCHAR(10),
#         Tour INTEGER,
#         Date DATE,
#         Time TIME,
#         Opponent VARCHAR(100),
#         HoAw VARCHAR(10),
#         ArsenalScore INTEGER,
#         OpponentScore INTEGER,
#         Stadium VARCHAR(100),
#         Attendance INTEGER,
#         Coach VARCHAR(100),
#         Referee VARCHAR(100)
#     """

#     # 2. Schema and column types for goalkeepers.csv
#     goalkeepers_schema = StructType([
#         StructField("LastName", StringType(), True),
#         StructField("FirstName", StringType(), True),
#         StructField("Date", StringType(), True),
#         StructField("Start", IntegerType(), True),
#         StructField("Pos", StringType(), True),
#         StructField("Min", IntegerType(), True),
#         StructField("SoTA", IntegerType(), True),
#         StructField("GA", IntegerType(), True),
#         StructField("Saves", IntegerType(), True),
#         StructField("PSxG", FloatType(), True),
#         StructField("PKatt", IntegerType(), True),
#         StructField("PKA", IntegerType(), True),
#         StructField("PKm", IntegerType(), True),
#         StructField("PassAtt", IntegerType(), True),
#         StructField("Throws", IntegerType(), True),
#         StructField("AvgLen", FloatType(), True),
#         StructField("GKAtt", IntegerType(), True),
#         StructField("GKAvgLen", FloatType(), True),
#         StructField("C", IntegerType(), True)
#     ])

#     goalkeepers_column_types = """
#         LastName VARCHAR(50),
#         FirstName VARCHAR(50),
#         Date DATE,
#         Start INTEGER,
#         Pos VARCHAR(10),
#         Min INTEGER,
#         SoTA INTEGER,
#         GA INTEGER,
#         Saves INTEGER,
#         PSxG FLOAT,
#         PKatt INTEGER,
#         PKA INTEGER,
#         PKm INTEGER,
#         PassAtt INTEGER,
#         Throws INTEGER,
#         AvgLen FLOAT,
#         GKAtt INTEGER,
#         GKAvgLen FLOAT,
#         C INTEGER
#     """

#     # 3. Schema and column types for players.csv
#     players_schema = StructType([
#         StructField("LastName", StringType(), True),
#         StructField("FirstName", StringType(), True),
#         StructField("Date", StringType(), True),
#         StructField("Start", IntegerType(), True),
#         StructField("Pos", StringType(), True),
#         StructField("Min", IntegerType(), True),
#         StructField("G", IntegerType(), True),
#         StructField("A", IntegerType(), True),
#         StructField("PK", IntegerType(), True),
#         StructField("PKA", IntegerType(), True),
#         StructField("S", IntegerType(), True),
#         StructField("SoT", IntegerType(), True),
#         StructField("YK", IntegerType(), True),
#         StructField("RK", IntegerType(), True),
#         StructField("Touches", IntegerType(), True),
#         StructField("Tackles", IntegerType(), True),
#         StructField("Ints", IntegerType(), True),
#         StructField("Blocks", IntegerType(), True),
#         StructField("xG", FloatType(), True),
#         StructField("npxG", FloatType(), True),
#         StructField("xAG", FloatType(), True),
#         StructField("Passes", IntegerType(), True),
#         StructField("PassesA", IntegerType(), True),
#         StructField("PrgPas", IntegerType(), True),
#         StructField("Carries", IntegerType(), True),
#         StructField("PrgCar", IntegerType(), True),
#         StructField("Line", StringType(), True),
#         StructField("C", IntegerType(), True)
#     ])

#     players_column_types = """
#         LastName VARCHAR(50),
#         FirstName VARCHAR(50),
#         Date DATE,
#         Start INTEGER,
#         Pos VARCHAR(10),
#         Min INTEGER,
#         G INTEGER,
#         A INTEGER,
#         PK INTEGER,
#         PKA INTEGER,
#         S INTEGER,
#         SoT INTEGER,
#         YK INTEGER,
#         RK INTEGER,
#         Touches INTEGER,
#         Tackles INTEGER,
#         Ints INTEGER,
#         Blocks INTEGER,
#         xG FLOAT,
#         npxG FLOAT,
#         xAG FLOAT,
#         Passes INTEGER,
#         PassesA INTEGER,
#         PrgPas INTEGER,
#         Carries INTEGER,
#         PrgCar INTEGER,
#         Line VARCHAR(20),
#         C INTEGER
#     """

#     # Date columns and their formats for parsing
#     matches_date_columns = [('Date', 'yyyy-MM-dd')]
#     goalkeepers_date_columns = [('Date', 'M/d/yyyy')]
#     players_date_columns = [('Date', 'M/d/yyyy')]

#     # Process each CSV file
#     read_and_write_csv_to_postgres(
#         spark, matches_csv_path, matches_db_table,
#         matches_schema, matches_column_types, matches_date_columns
#     )
#     read_and_write_csv_to_postgres(
#         spark, goalkeepers_csv_path, goalkeepers_db_table,
#         goalkeepers_schema, goalkeepers_column_types, goalkeepers_date_columns
#     )
#     read_and_write_csv_to_postgres(
#         spark, players_csv_path, players_db_table,
#         players_schema, players_column_types, players_date_columns
#     )

# if __name__ == "__main__":
#     main()

