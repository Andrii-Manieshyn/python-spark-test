from pyspark import SparkContext
from pyspark.sql import SparkSession


SPARK_MASTER = "local[2]"
APP_NAME = "adTestProject"

def join_two_df(df_one, df_two, fields):
    return df_one.join(df_two, fields, how="left")

def get_spark_session():
    sc = SparkContext(SPARK_MASTER, APP_NAME)
    return SparkSession(sc)
