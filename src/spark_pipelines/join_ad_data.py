from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from pyspark import Row
import os
import shutil

SPARK_MASTER = "local[2]"
APP_NAME = "adTestProject"

IMPRESSION_FILE = "../../resources/impression_log.txt"
CLICK_LOG_FILE = "../../resources/click_log.txt"

IMPRESSION_COLUMNS = ("timestamp", "customerId", "cookie", "auctionId")
CLICK_LOG_COLUMNS = ("timestamp", "auctionId")

if (os.path.isdir("../../resources/output/result")):
    shutil.rmtree("../../resources/output/result")

sc = SparkContext (SPARK_MASTER, APP_NAME)
spark = SparkSession(sc)

impresssion_file = sc.textFile(IMPRESSION_FILE)
click_log_file = sc.textFile(CLICK_LOG_FILE)

df_impressions = spark.read.option("header", "true") \
    .option("delimiter", " ") \
    .option("inferSchema", "true") \
    .csv(IMPRESSION_FILE)

df_impressions.show(truncate=False)

df_click_log=spark.read.option("header", "true") \
    .option("delimiter", " ") \
    .option("inferSchema", "true") \
    .csv(CLICK_LOG_FILE)

df_click_log.show(truncate=False)

#joined_tables = df_click_log.join(df_impressions, df_click_log.auctionId == df_impressions.auctionId)
joined_tables = df_click_log.join(df_impressions, ["auctionid", "timestamp"], how="left")
joined_tables.show(truncate=False)
joined_tables.write.csv("../../resources/output/result", header=True, sep=" ")
#joined_tables.toPandas().to_csv("../../resources/output/result.csv")


