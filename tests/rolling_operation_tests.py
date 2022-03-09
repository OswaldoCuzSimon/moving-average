import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, DoubleType, DateType

from indicators.rolling_operations import calculate_moving_average


class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[1]")
                     .appName("PySpark unit test")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class MovingAverageTestCase(PySparkTestCase):

    def test_dataparser_schema(self):
        input_schema = StructType([
            StructField('ticker', StringType(), True),
            StructField('close', DoubleType(), True),
            StructField('date', StringType(), True)
        ])
        output_schema = StructType([
            StructField('ticker', StringType(), True),
            StructField('close', DoubleType(), True),
            StructField('date', StringType(), True),
            StructField('moving_average_7', DoubleType(), True)
        ])

        input_df = self.spark.createDataFrame(
            data=[
                ['GOOGL', 1224.06005859375, '2018-08-16'],
                ['GOOGL', 1215.84997558594, '2018-08-17'],
                ['GOOGL', 1221.94995117188, '2018-08-20'],
                ['GOOGL', 1217.41003417969, '2018-08-21'],
                ['GOOGL', 1221.75, '2018-08-22'],
                ['GOOGL', 1221.16003417969, '2018-08-23'],
                ['GOOGL', 1236.75, '2018-08-24']],
            schema=input_schema)
        expected_df = self.spark.createDataFrame(
            data=[
                ['GOOGL', 1224.06005859375, '2018-08-16', None],
                ['GOOGL', 1215.84997558594, '2018-08-17', None],
                ['GOOGL', 1221.94995117188, '2018-08-20', None],
                ['GOOGL', 1217.41003417969, '2018-08-21', None],
                ['GOOGL', 1221.75, '2018-08-22', None],
                ['GOOGL', 1221.16003417969, '2018-08-23', None],
                ['GOOGL', 1236.75, '2018-08-24', 1222.7042933872785]],
            schema=output_schema)

        output_df = calculate_moving_average(input_df, 'close', 'GOOGL', 7, 'moving_average_7')
        output_moving_average = output_df.select("moving_average_7").rdd.flatMap(lambda x: x).collect()
        expected_moving_average = expected_df.select("moving_average_7").rdd.flatMap(lambda x: x).collect()
        self.assertListEqual(expected_moving_average, output_moving_average)


if __name__ == '__main__':
    unittest.main()
