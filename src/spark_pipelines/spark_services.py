from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions

SPARK_MASTER = "local[2]"
APP_NAME = "adTestProject"

def join_two_df(df_one, df_two, fields):
    return df_one.join(df_two, fields, how="left")

def complicated_computation_two_df(df_one, df_two):
    return df_one.select("timestamp", "auctionId").unionByName(df_two.select("timestamp", "auctionId"))


def complicated_computation_one_df(df_one):
    return df_one.filter(df_one.timestamp % 15 == 0)



def get_spark_session():
    sc = SparkContext(SPARK_MASTER, APP_NAME)
    return SparkSession(sc)
