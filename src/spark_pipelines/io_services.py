import os
import shutil

def read_df_from_file(spark_session ,file_name):
    return spark_session.read.option("header", "true") \
    .option("delimiter", " ") \
    .option("inferSchema", "true") \
    .csv(file_name)

def read_df_to_csv_file(df, path_to_file):
    df.write.csv(path_to_file, header=True, sep=" ")

def clean_target_directory():
    if (os.path.isdir("../../resources/output/result")):
        shutil.rmtree("../../resources/output/result")