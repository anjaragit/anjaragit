import unittest
import warnings
warnings.simplefilter("ignore")
# from main import extract_age_func
# from dataframe_test_utils import PySparkTestCase, test_schema, test_data


import unittest

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_extract, col

def extract_age_func(input_df: DataFrame, id_col: str):
    pattern = '\d+(?=-)'
    return input_df.withColumn('age', regexp_extract(col(id_col), pattern, 0))

class PySparkTestCase(unittest.TestCase):
    """Set-up of global test SparkSession"""

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local")
                     .appName("PySpark unit test")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


def test_schema(df1: DataFrame, df2: DataFrame, check_nullable=True):
    """
    Function for comparing two schemas of DataFrames. If schemas are equal returns True.
    :param df1: test DataFrame
    :param df2: test DataFrame
    :param check_nullable: flag for checking column nullability
    :return: Boolean
    """
    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, df1.schema.fields)]
    fields2 = [*map(field_list, df2.schema.fields)]
    if check_nullable:
        res = set(fields1) == set(fields2)
    else:
        res = set([field[:-1] for field in fields1]) == set([field[:-1] for field in fields2])
    return res


def test_data(df1: DataFrame, df2: DataFrame):
    """
    Function for comparing two DataFrame data. If data is equal returns True.
    :param df1: test DataFrame
    :param df2: test DataFrame
    :return: Boolean
    """
    data1 = df1.collect()
    data2 = df2.collect()
    return set(data1) == set(data2)


class SimpleTestCase(PySparkTestCase):

    def test_dataparser_schema(self):
        input_df = self.spark.createDataFrame(
            data=[['Jan', 'Janson', 'jj@email.com', '20-504123'],
                  ['Jen', 'Jenny', 'jen@email.com', '55-357378'],
                  ['Bill', 'Bill', 'bill@email.com', '79-357378']],
            schema=['first_name', 'last_name', 'email', 'id'])

        transformed_df = extract_age_func(input_df, "id")

        expected_df = self.spark.createDataFrame(
            data=[['Jan', 'Janson', 'jj@email.com', '20-504123', '20'],
                  ['Jen', 'Jenny', 'jen@email.com', '55-357378', '55'],
                  ['Bill', 'Bill', 'bill@email.com', '79-357378', '79']],
            schema=['first_name', 'last_name', 'email', 'id', 'age'])

        self.assertTrue(test_schema(transformed_df, expected_df))

    def test_dataparser_data(self):
        input_df = self.spark.createDataFrame(
            data=[['Jan', 'Janson', 'jj@email.com', '20-504123'],
                  ['Jen', 'Jenny', 'jen@email.com', '55-357378'],
                  ['Bill', 'Bill', 'bill@email.com', '79-357378']],
            schema=['first_name', 'last_name', 'email', 'id'])

        transformed_df = extract_age_func(input_df, "id")

        expected_df = self.spark.createDataFrame(
            data=[['Jan', 'Janson', 'jj@email.com', '20-504123', '20'],
                  ['Jen', 'Jenny', 'jen@email.com', '55-357378', '55'],
                  ['Bill', 'Bill', 'bill@email.com', '79-357378', '79']],
            schema=['first_name', 'last_name', 'email', 'id', 'age'])

        self.assertTrue(test_data(transformed_df, expected_df))

if __name__ == '__main__':
    unittest.main()