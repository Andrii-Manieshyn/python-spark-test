from unittest import TestCase

from pyspark import SparkContext
from pyspark.sql import SQLContext

from src.spark_pipelines.spark_services import join_two_df, complicated_computation_two_df, \
    complicated_computation_one_df

import pandas as pd

SPARK_MASTER = "local[2]"
APP_NAME = "adTestProject"

sc = SparkContext(SPARK_MASTER, APP_NAME)
sql_context = SQLContext(sc)

class TestCaseForMyPyspark(TestCase):



    def test_filter_spark_data_frame(self):
        df_input_one = create_first_df()

        df_input_two = create_second_df()

        expected_output = sql_context.createDataFrame(
            [("d444d9ee",1549654396,      None,             None),
             ("3f9ddfe4",1553032869,      None,             None),
             ("fdc17e39",1551354855,      2877,"4977111e-b838-4e0"),
             ("09aada0e",1551314912,      None,             None),
             ("78ac2788",1553925471,      None,             None),
             ("dc773a03",1549654396,      2836,"f8731171-10d5-432"),
             ],
            ["auctionId", "timestamp", "customerId", "cookie"],
        )

        real_output = join_two_df(df_input_two, df_input_one,  ["auctionid", "timestamp"])
        real_output.show()
        real_output = get_sorted_data_frame(
            real_output.toPandas(),
            ["auctionId", "timestamp", "customerId", "cookie"],
        )
        expected_output = get_sorted_data_frame(
            expected_output.toPandas(),
            ["auctionId", "timestamp", "customerId", "cookie"],
        )


        pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)



    def test_complicated_computation_two_df(self):

        df_input_one = create_first_df()

        df_input_two = create_second_df()

        expected_output = sql_context.createDataFrame(
            [(1551314912,"09aada0e"),
             (1551354855,"fdc17e39"),
             (1549654396,"dc773a03"),
             (1549654396,"d444d9ee"),
             (1553925471,"78ac2788"),
             (1553032869,"3f9ddfe4"),
             (1551698263,"d708e626"),
             (1551354855,"fdc17e39"),
             (1549654396,"dc773a03"),
             (1551285656,"ec419f22"),
             (1553925471,"9e00778f"),
             (1553622986,"5561f9c6"),
             ],
            ["timestamp", "auctionId"],
        )

        real_output = complicated_computation_two_df(df_input_two, df_input_one)
        real_output.show()
        real_output = get_sorted_data_frame(
            real_output.toPandas(),
            ["timestamp", "auctionId"],
        )
        expected_output = get_sorted_data_frame(
            expected_output.toPandas(),
            ["timestamp", "auctionId"],
        )


        pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)

    def test_complicated_computation_one_df(self):

        df_input_one = create_first_df()

        expected_output = sql_context.createDataFrame(
            [(1551354855, 2877, "4977111e-b838-4e0", "fdc17e39")],
            ["timestamp", "customerId", "cookie", "auctionId"],
        )

        real_output = complicated_computation_one_df(df_input_one)
        real_output.show()
        real_output = get_sorted_data_frame(
            real_output.toPandas(),
            ["timestamp", "customerId", "cookie", "auctionId"],
        )
        expected_output = get_sorted_data_frame(
            expected_output.toPandas(),
            ["timestamp", "customerId", "cookie", "auctionId"],
        )


        pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)


def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)

def create_second_df():
    return sql_context.createDataFrame(
        [(1551314912, "09aada0e"),
         (1551354855, "fdc17e39"),
         (1549654396, "dc773a03"),
         (1549654396, "d444d9ee"),
         (1553925471, "78ac2788"),
         (1553032869, "3f9ddfe4")],
        ["timestamp", "auctionId"],
    )

def create_first_df():
    return sql_context.createDataFrame(
        [(1551698263, 2678, "d0473f70-3e7f-416", "d708e626"),
         (1551354855, 2877, "4977111e-b838-4e0", "fdc17e39"),
         (1549654396, 2836, "f8731171-10d5-432", "dc773a03"),
         (1551285656, 2236, "d2cb716a-2a15-45c ", "ec419f22"),
         (1553925471, 2111, "e2686169-b03a-412", " 9e00778f"),
         (1553622986, 2411, "24a4a6fc-3e12-40d", "5561f9c6"),
         ],
        ["timestamp", "customerId", "cookie", "auctionId"],
    )