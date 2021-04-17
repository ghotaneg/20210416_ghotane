import os
from os.path import abspath
import re
import shutil
import pyspark
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func


def question_1():
    ans_df = spark.sql("""
        SELECT servedCuisines, placeId, totalSalesAmount 
        FROM (
            SELECT p.servedCuisines, p.placeId, SUM(f.salesAmount) as totalSalesAmount, ROW_NUMBER() OVER(PARTITION BY servedCuisines ORDER BY SUM(f.salesAmount) DESC) as rnk
            FROM place_cuisines p
            INNER JOIN fct_user_place_interaction f
            ON p.placeId = f.placeId
            WHERE p.servedCuisines != 'unknown'
            GROUP BY p.servedCuisines, p.placeId
        ) c
        WHERE rnk <= 3
    """)

    ans_df.write.parquet(OUTPUT_DIR + "question_1")


def question_2(n):

    ans_df = spark.sql("""
        SELECT servedCuisines, placeId, totalSalesAmount 
        FROM (
            SELECT p.servedCuisines, p.placeId, SUM(f.salesAmount) as totalSalesAmount, ROW_NUMBER() OVER(PARTITION BY servedCuisines ORDER BY SUM(f.salesAmount) DESC) as rnk
            FROM place_cuisines p
            INNER JOIN fct_user_place_interaction f
            ON p.placeId = f.placeId
            WHERE p.servedCuisines != 'unknown'
            GROUP BY p.servedCuisines, p.placeId
        ) c
        WHERE rnk = {}
    """.format(n))

    ans_df.write.parquet(OUTPUT_DIR + "question_2")


def question_3(visitDate):

    year = int(visitDate[:4])
    month = int(visitDate[5:7])
    day = int(visitDate[8:10])

    ans_df = spark.sql("""
        SELECT userId, CASE WHEN COUNT(*) = 1 THEN -9994 ELSE AVG(diff) END as avg_hrs 
        FROM (
            SELECT f.userId, f.placeId, f.visitDateTime, LEAD(f.visitDateTime) OVER (PARTITION BY f.userId ORDER BY f.visitDateTime) as nextVisitTime,
                (CAST(LEAD(f.visitDateTime) OVER (PARTITION BY f.userId ORDER BY f.visitDateTime) AS LONG) - CAST(f.visitDateTime AS LONG))/3600 as diff
            FROM fct_user_place_interaction f
            WHERE f.year = {} AND f.month = {} AND f.day = {}
        ) a
        GROUP BY userId
        ORDER BY 1
    """.format(year, month, day))

    ans_df.write.parquet(OUTPUT_DIR + "question_3")


if __name__ == "__main__":
    warehouse_location = abspath('/home/ubuntu/workspace/spark-warehouse/')

    spark = SparkSession.builder\
        .appName("ETL User Details")\
        .master("local[*]")\
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    config = ConfigParser()
    config.read("/home/ubuntu/workspace/configs/config.ini")

    OUTPUT_DIR = config['INFO']['OUTPUT_DIR']
    SCRIPTS_DIR = config['INFO']['SCRIPTS_DIR']

    question_1()
    question_2(3)
    question_3("2020-05-10")
