from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, row_number
from pyspark.sql.types import StructType, StringType, StructField, DoubleType, IntegerType, DateType
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()
spark = SparkSession.builder.appName("name").getOrCreate()


file_type = 'csv'
file_location = "historical_stock_prices.csv.gz"
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
customSchema = StructType([
    StructField('ticker', StringType(), True),
    StructField('open', DoubleType(), True),
    StructField('close', DoubleType(), True),
    StructField('adj_close', DoubleType(), False),
    StructField('low', DoubleType(), True),
    StructField('high', DoubleType(), True),
    StructField('volume', IntegerType(), True),
    StructField('date', DateType(), True)])

df = spark.read.format(file_type) \
               .option("header", first_row_is_header) \
               .option("sep", delimiter) \
               .schema(customSchema) \
               .load(file_location)


def calculate_moving_average(df, column, ticker, n, moving_average_column_name):
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


ticker = 'GOOGL'
df = df.drop_duplicates()
df = calculate_moving_average(df, 'close', ticker, 7, 'moving_average_7')
df.count()
df.repartition(1).write.option("header", "true").csv("{}".format(ticker))