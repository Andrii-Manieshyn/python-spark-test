import os
import shutil

OUTPUT_RESULT_FOLDER = "../resources/output/result"

def read_df_from_file(spark_session ,file_name):
    return spark_session.read.option("header", "true") \
    .option("delimiter", " ") \
    .option("inferSchema", "true") \
    .csv(file_name)

def write_df_to_csv_file(df, path_to_file):
    clean_target_directory()
    df.write.csv(path_to_file, header=True, sep=" ")

def clean_target_directory():
    if (os.path.isdir(OUTPUT_RESULT_FOLDER)):
        shutil.rmtree(OUTPUT_RESULT_FOLDER)