from src.spark_pipelines.io_services import *
from src.spark_pipelines.spark_services import *



IMPRESSION_FILE = "../../resources/impression_log.txt"
CLICK_LOG_FILE = "../../resources/click_log.txt"

clean_target_directory()

spark = get_spark_session()

df_impressions = read_df_from_file(spark, IMPRESSION_FILE)

df_click_log=read_df_from_file(spark, CLICK_LOG_FILE)

joined_tables=join_two_df(df_click_log, df_impressions, ["auctionid", "timestamp"])

read_df_to_csv_file(joined_tables, "../../resources/output/result")


