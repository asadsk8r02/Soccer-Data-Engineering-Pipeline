from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import (
    col, to_date, date_format, year, month, dayofmonth, dayofweek, quarter,
    expr, min, max, lit, monotonically_increasing_id, concat_ws
)

def create_spark_session():
    spark = SparkSession.builder.config("spark.jars", "/Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar").getOrCreate()
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
    Matches.createOrReplaceTempView("Matches")
    DimMatch= Matches.withColumn("MatchID", monotonically_increasing_id())
    DimMatch = DimMatch.withColumn("FormattedDate", date_format(to_date("Date", "yyyy-M-d"), "yyyy-MM-dd"))

    ## Load DimMatch to DWH 
    DimMatch.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "dwh.DimArsenalMatches") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("overwrite") \
    .save()

    return DimMatch

def transform_load_Players_data(spark, dataframe,DimMatch):

    Players = dataframe['arsenalplayers']
    Players.createOrReplaceTempView("Players")

    distinct_players= spark.sql("""
    select distinct concat(firstname, " ", lastname) as fullname
    from Players

    """)
    
    players_Dates= spark.sql("""
    select count(distinct Date) 
    from Players

    """)
    

    distinct_players= distinct_players.withColumn("PlayerID", monotonically_increasing_id())

    Players= Players.withColumn('fullname', concat_ws(" ", col('FirstName'),col('LastName')))
    Players.select("fullname").show(5, False)

    DimPlayers= Players.join(distinct_players, on ='fullname', how="inner")
    DimPlayers.columns

    # For DimPlayers with original format M/d/yyyy
    DimPlayers = DimPlayers.withColumn("FormattedDate", date_format(to_date("Date", "M/d/yyyy"), "yyyy-MM-dd"))
    FactPlayers = DimMatch.join(DimPlayers, on='FormattedDate', how= 'left')


    FactPlayers = FactPlayers.drop('Date')

    # Register the DataFrame as a temporary view
    FactPlayers.createOrReplaceTempView("fact_players")

    # SQL query to select rows with any null values
    query = "SELECT * FROM fact_players WHERE " + ' OR '.join([f"{column} IS NULL" for column in FactPlayers.columns])

    # Execute the query
    rows_with_nulls_sql = spark.sql(query)

   

    FactPlayers = FactPlayers.drop('Season',
    'Tour',
    'Time',
    'Opponent',
    'HoAw',
    'Stadium','Coach',
    'Referee',
    'fullname',
    'LastName',
    'FirstName','Line')

    #Loading FactPlayers to DWH
    FactPlayers.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.FactArsenalPlayers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()
    
    DimPlayers= DimPlayers.drop('Date',
    'Start',
    'Min',
    'G',
    'A',
    'PK',
    'PKA',
    'S',
    'SoT',
    'YK',
    'RK',
    'Touches',
    'Tackles',
    'Ints',
    'Blocks',
    'xG',
    'npxG',
    'xAG',
    'Passes',
    'PassesA',
    'PrgPas',
    'Carries',
    'PrgCar',
    'Line',
    'C','FormattedDate', 'Pos')

    ## Loading DimPlayers to DWH 
    DimPlayers.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "dwh.DimArsenalPlayers") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("overwrite") \
    .save()

    return DimPlayers,FactPlayers

def transform_GK_data(spark, dataframe, DimMatch):

    GoalKeepers = dataframe['arsenalgk']
    GoalKeepers.createOrReplaceTempView("GK")

    GoalKeepers= GoalKeepers.withColumn('fullname', concat_ws(" ", col('FirstName'),col('LastName')))

    GoalKeepers_f = spark.sql("""
        select distinct concat(firstname, " ", lastname) as fullname
        from GK

    """)

    GoalKeepers_f= GoalKeepers_f.withColumn('GkID',monotonically_increasing_id()+1)

    DimGoalKeepers= GoalKeepers.join(GoalKeepers_f, on ='fullname', how="inner")

    DimGoalKeepers = DimGoalKeepers.withColumn("FormattedDate", date_format(to_date("Date", "M/d/yyyy"), "yyyy-MM-dd"))

    FactGk = DimMatch.join(DimGoalKeepers, on='FormattedDate', how='left')

    FactGk = FactGk.drop( 
    'Season',
    'Tour',
    'Time',
    'Opponent',
    'HoAw',
    'Stadium',
    'Coach',
    'Referee',
    'Pos',
    'fullname',
    'LastName',
    'FirstName','Date'
    
    )
    
    ## Loading FactGK to DWH
    FactGk.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.FactArsenalGoalKeepers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

    DimGoalKeepers= DimGoalKeepers.drop('Min','Start',
    'SoTA',
    'GA',
    'Saves',
    'PSxG',
    'PKatt',
    'PKA',
    'PKm',
    'PassAtt',
    'Throws',
    'AvgLen',
    'GKAtt',
    'GKAvgLen','Date','C','FormattedDate')


    DimGoalKeepers.createOrReplaceTempView("DimGoalKeepers")
    DimGoalKeepers = spark.sql(""" select distinct * from DimGoalKeepers""")
    DimGoalKeepers =DimGoalKeepers.dropDuplicates()

    ##Loading DimGK to DWH
    DimGoalKeepers.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimArsenalGoalKeepers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()
    return DimGoalKeepers,FactGk


# def create_and_load_dim_date(spark, date_df):
#     # Transform the source DataFrame to create the dim_date DataFrame
#     date_df = spark.range(date_diff + 1).select(to_date(expr(f"date_add(to_date('{min_date}', 'yyyy-MM-dd'), cast(id as int))"), "yyyy-MM-dd").alias("Date"))
    
#     dim_date_df = date_df.select(
#         "Date",
#         year("Date").alias("Year"),
#         month("Date").alias("Month"),
#         dayofmonth("Date").alias("Day"),
#         dayofweek("Date").alias("Weekday"),
#         quarter("Date").alias("Quarter")
#     )
    
#     # Load the dim_date DataFrame to the PostgreSQL database
#     dim_date_df.write.format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
#         .option("driver", "org.postgresql.Driver") \
#         .option("dbtable", "dwh.DimDate") \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .mode("overwrite") \
#         .save()
    
#     # Return the transformed DataFrame
#     return dim_date_df

# # To use this function, call it with the Spark session and source DataFrame that contains the Date column.
# # For example:
# # spark = create_spark_session()
# # source_df = spark.read...  # load your source data with a 'Date' column
# # dim_date_df = create_and_load_dim_date(spark, source_df)

    

#     # Additional transformations can be added here as needed

# def create_and_load_dim_date(spark):
#     """
#     Create the dim_date DataFrame and load it into a PostgreSQL table.
#     """
#     # Example logic to calculate min_date and date_diff
#     # You'll need to replace this with actual logic to determine these values
#     min_date = '2017-08-11'
#     max_date = '2023-02-25'
#     date_diff = (to_date(lit(max_date), 'yyyy-MM-dd') - to_date(lit(min_date), 'yyyy-MM-dd')).days

#     # Now create the date_df DataFrame
#     date_df = spark.range(date_diff + 1).select(expr(f"date_add(to_date('{min_date}', 'yyyy-MM-dd'), id)").alias("Date"))
    
#     # Create the dim_date DataFrame with additional date parts
#     dim_date_df = date_df.select(
#         "Date",
#         year("Date").alias("Year"),
#         month("Date").alias("Month"),
#         dayofmonth("Date").alias("Day"),
#         dayofweek("Date").alias("Weekday"),
#         quarter("Date").alias("Quarter")
#     )
    
#     # Load the dim_date DataFrame to the PostgreSQL database
#     dim_date_df.write.format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
#         .option("driver", "org.postgresql.Driver") \
#         .option("dbtable", "dwh.DimDate") \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .mode("overwrite") \
#         .save()

def create_and_load_dim_date(spark, source_df):
    """
    Create the dim_date DataFrame based on a source DataFrame and load it into a PostgreSQL table.
    """
    # Calculate min_date and max_date dynamically from the source DataFrame
    min_date = source_df.agg({"Date": "min"}).collect()[0][0]
    max_date = source_df.agg({"Date": "max"}).collect()[0][0]
    
    # Calculate the difference in days
    date_diff = (max_date - min_date).days

    # Create the date_df DataFrame
    date_df = spark.range(date_diff + 1).select(expr(f"date_add(to_date('{min_date}', 'yyyy-MM-dd'), id)").alias("Date"))

    # Create the dim_date DataFrame with additional date parts
    dim_date_df = date_df.select(
        "Date",
        year("Date").alias("Year"),
        month("Date").alias("Month"),
        dayofmonth("Date").alias("Day"),
        dayofweek("Date").alias("Weekday"),
        quarter("Date").alias("Quarter")
    )

    # Load the dim_date DataFrame to the PostgreSQL database
    dim_date_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimDate") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

    # Return the transformed DataFrame
    return dim_date_df

# def create_and_load_dim_date(spark, source_df):
#     """
#     Create the dim_date DataFrame based on a source DataFrame and load it into a PostgreSQL table.
#     """
#     # Ensure the Date column is of DateType
#     source_df = source_df.withColumn("Date", to_date(col("Date"), "your_date_format"))
    
#     # Filter out null dates if necessary
#     source_df = source_df.filter(col("Date").isNotNull())
    
#     # Calculate min_date and max_date dynamically from the source DataFrame
#     min_date = source_df.agg({"Date": "min"}).collect()[0][0]
#     max_date = source_df.agg({"Date": "max"}).collect()[0][0]
    
#     # Check if min_date and max_date are not None
#     if min_date is None or max_date is None:
#         raise ValueError("min_date or max_date is None. Check your Date column for valid dates.")
    
#     # Convert dates to strings
#     min_date_str = min_date.strftime('%Y-%m-%d')
#     max_date_str = max_date.strftime('%Y-%m-%d')
    
#     # Calculate the difference in days
#     date_diff = (max_date - min_date).days

#     # Create the date_df DataFrame
#     date_df = spark.range(date_diff + 1).select(expr(f"date_add(to_date('{min_date_str}', 'yyyy-MM-dd'), id)").alias("Date"))

#     # Create the dim_date DataFrame with additional date parts
#     dim_date_df = date_df.select(
#         "Date",
#         year("Date").alias("Year"),
#         month("Date").alias("Month"),
#         dayofmonth("Date").alias("Day"),
#         dayofweek("Date").alias("Weekday"),
#         quarter("Date").alias("Quarter")
#     )

#     # Load the dim_date DataFrame to the PostgreSQL database
#     dim_date_df.write.format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
#         .option("driver", "org.postgresql.Driver") \
#         .option("dbtable", "dwh.DimDate") \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .mode("overwrite") \
#         .save()

#     # Return the transformed DataFrame
#     return dim_date_df



# Adjust the main function accordingly
def main():
    spark = create_spark_session()
    
    tbl_list = ['arsenalmatches', 'arsenalplayers', 'arsenalgk']
    dataframe = load_data_from_postgres(spark, tbl_list)

    DimMatch = transform_load_matches_data(spark, dataframe)

    transform_load_Players_data(spark, dataframe, DimMatch)
    
    transform_GK_data(spark, dataframe, DimMatch)
    # create_and_load_dim_date(spark)  # Adjusted call without date_df
    create_and_load_dim_date(spark, DimMatch) 

if __name__ == "__main__":
    main()
