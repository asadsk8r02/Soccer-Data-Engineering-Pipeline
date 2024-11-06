from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, date_format, year, month, dayofmonth, dayofweek, quarter,
    min as spark_min, max as spark_max, dense_rank, concat_ws
)
from pyspark.sql.window import Window

def create_spark_session():
    spark = SparkSession.builder \
        .config("spark.jars", "/Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar") \
        .getOrCreate()
    return spark

def load_data_from_postgres(spark, tbl_list):
    dataframe = {}
    for table in tbl_list:
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"{table}") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .load()
        dataframe[table] = df
    return dataframe

def transform_load_matches_data(spark, dataframe):
    Matches = dataframe['arsenalmatches']
    DimMatch = Matches.withColumn("MatchID", dense_rank().over(Window.orderBy("Date"))) \
        .withColumn("FormattedDate", date_format(to_date("Date", "yyyy-M-d"), "yyyy-MM-dd"))

    DimMatch.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimArsenalMatches") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

    return DimMatch

def transform_load_Players_data(spark, dataframe, DimMatch):
    Players = dataframe['arsenalplayers']
    Players = Players.withColumn('fullname', concat_ws(" ", 'FirstName', 'LastName'))

    window_spec = Window.partitionBy("fullname").orderBy("fullname")
    DimPlayers = Players.withColumn("PlayerID", dense_rank().over(window_spec)) \
        .withColumn("FormattedDate", date_format(to_date("Date", "M/d/yyyy"), "yyyy-MM-dd"))

    FactPlayers = DimPlayers.join(DimMatch, on='FormattedDate', how='left') \
        .drop('Date', 'Season', 'Tour', 'Time', 'Opponent', 'HoAw', 'Stadium', 'Coach', 
              'Referee', 'fullname', 'LastName', 'FirstName', 'Line')

    FactPlayers.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.FactArsenalPlayers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

    DimPlayers = DimPlayers.drop('Date', 'Start', 'Min', 'G', 'A', 'PK', 'PKA', 'S', 'SoT',
                                 'YK', 'RK', 'Touches', 'Tackles', 'Ints', 'Blocks', 'xG',
                                 'npxG', 'xAG', 'Passes', 'PassesA', 'PrgPas', 'Carries',
                                 'PrgCar', 'Line', 'C', 'FormattedDate', 'Pos').dropDuplicates()

    DimPlayers.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimArsenalPlayers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

    return DimPlayers, FactPlayers

def transform_GK_data(spark, dataframe, DimMatch):
    GoalKeepers = dataframe['arsenalgk']
    GoalKeepers = GoalKeepers.withColumn('fullname', concat_ws(" ", 'FirstName', 'LastName'))

    window_spec = Window.partitionBy("fullname").orderBy("fullname")
    DimGoalKeepers = GoalKeepers.withColumn("GkID", dense_rank().over(window_spec)) \
        .withColumn("FormattedDate", date_format(to_date("Date", "M/d/yyyy"), "yyyy-MM-dd"))

    FactGk = DimGoalKeepers.join(DimMatch, on='FormattedDate', how='left') \
        .drop('Date', 'Season', 'Tour', 'Time', 'Opponent', 'HoAw', 'Stadium', 'Coach',
              'Referee', 'Pos', 'fullname', 'LastName', 'FirstName')

    FactGk.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.FactArsenalGoalKeepers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

    DimGoalKeepers = DimGoalKeepers.drop('Min', 'Start', 'SoTA', 'GA', 'Saves', 'PSxG',
                                         'PKatt', 'PKA', 'PKm', 'PassAtt', 'Throws',
                                         'AvgLen', 'GKAtt', 'GKAvgLen', 'Date', 'C',
                                         'FormattedDate').dropDuplicates()

    DimGoalKeepers.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimArsenalGoalKeepers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()
    return DimGoalKeepers, FactGk

def create_and_load_dim_date(spark, source_df):
    date_format_string = "yyyy-MM-dd"
    date_df = source_df.select(to_date("FormattedDate", date_format_string).alias("Date"))
    min_date = date_df.agg(spark_min("Date")).first()[0]
    max_date = date_df.agg(spark_max("Date")).first()[0]

    date_diff = (max_date - min_date).days
    date_range = spark.range(0, date_diff + 1) \
        .withColumn("Date", expr(f"date_add('{min_date}', id)"))

    dim_date_df = date_range.select(
        "Date",
        year("Date").alias("Year"),
        month("Date").alias("Month"),
        dayofmonth("Date").alias("Day"),
        dayofweek("Date").alias("Weekday"),
        quarter("Date").alias("Quarter")
    )

    dim_date_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimDate") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

    return dim_date_df

def main():
    spark = create_spark_session()
    
    tbl_list = ['arsenalmatches', 'arsenalplayers', 'arsenalgk']
    dataframe = load_data_from_postgres(spark, tbl_list)

    DimMatch = transform_load_matches_data(spark, dataframe)
    transform_load_Players_data(spark, dataframe, DimMatch)
    transform_GK_data(spark, dataframe, DimMatch)
    create_and_load_dim_date(spark, DimMatch) 

if __name__ == "__main__":
    main()
