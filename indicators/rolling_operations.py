import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, row_number
from pyspark.sql.types import StructType, StringType, StructField, DoubleType, IntegerType, DateType
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()
spark = (SparkSession.builder.master("local[*]").appName("moving_average")
         .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2')
         .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
         .getOrCreate())


def read_csv(file_location, customSchema, delimiter=","):
    """
    Read csv and load into DataFrame
    Parameters
    ----------
    file_location : str
        Path of a csv to load
    customSchema : StructType
        Schema of the dataframe
    delimiter : str
        Delimiter of the csv file

    Returns
    -------
    DataFrame
        Data frame representation of the csv loaded
    """

    df = spark.read.format('csv') \
                   .option("header", "true") \
                   .option("sep", delimiter) \
                   .schema(customSchema) \
                   .load(file_location)
    return df


def calculate_moving_average(df, column, ticker, n, moving_average_column_name):
    """
    Calculate moving average
    Parameters
    ----------
    df : DataFrame
        Data frame with OHLC price data
    column : str
        Column name of time serie to apply moving average
    ticker : str
        Ticker symbol to calculate moving average
    n : int
        Number of periods
    moving_average_column_name : str
        Name of the column with the result

    Returns
    -------
    DataFrame
        The dataframe received plus a column named moving_average_column_name with the moving average
    """
    rolling_function = Window.partitionBy('ticker') \
        .orderBy("date") \
        .rowsBetween(-n, 0)
    df = (df.where("ticker =='{}'".format(ticker))
          .orderBy("date", ascending=True)
          .withColumn(moving_average_column_name, F.avg(column).over(rolling_function)))
    row_number_function = Window().partitionBy('ticker').orderBy(lit('a'))
    df = df.withColumn("row_num", row_number().over(row_number_function))\
        .withColumn(
            moving_average_column_name, when(col("row_num") < n, lit(None)).otherwise(col(moving_average_column_name)))\
        .drop('row_num')

    return df


custom_schema = StructType([
    StructField('ticker', StringType(), True),
    StructField('open', DoubleType(), True),
    StructField('close', DoubleType(), True),
    StructField('adj_close', DoubleType(), False),
    StructField('low', DoubleType(), True),
    StructField('high', DoubleType(), True),
    StructField('volume', IntegerType(), True),
    StructField('date', DateType(), True)
])


file_location = "historical_stock_prices.csv.gz"
file_location = "s3a://kueski-challenge-data-engineer/historical_stock_prices.csv.gz"
df = read_csv(file_location, custom_schema, delimiter=",")

ticker = 'GOOGL'
df = df.drop_duplicates()
df = calculate_moving_average(df, 'close', ticker, 7, 'moving_average_7')
df.count()
df.repartition(1).write.option("header", "true").csv("{}".format(ticker))