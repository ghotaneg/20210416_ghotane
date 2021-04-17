import os
from os.path import abspath
import re
import shutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func


def populate_exploding_dims(dataframe, tgt_table_name, key_col_name, exp_col_name):
    df = dataframe[[key_col_name, exp_col_name]]\
        .withColumn(exp_col_name, func.explode(exp_col_name))\
        .withColumn(exp_col_name, func.lower(func.regexp_replace(exp_col_name, '-', '_')))

    df_nulls = dataframe.select([key_col_name, exp_col_name]).where(
        '{} IS NULL'.format(exp_col_name))
    df_nulls = df_nulls.withColumn(exp_col_name, func.lit('unknown'))
    new_data_df = df.union(df_nulls)
    new_data_df = new_data_df\
        .withColumn(key_col_name, new_data_df[key_col_name].cast(IntegerType()))
    existing_df = spark.sql("SELECT * FROM {}".format(tgt_table_name))
    old_data_df = existing_df.join(new_data_df, existing_df[key_col_name] == new_data_df[key_col_name], 'left').where(
        new_data_df[key_col_name].isNull()).select(existing_df['*'])
    final_data_df = old_data_df.union(new_data_df)

    try:
        shutil.rmtree(warehouse_location + "/tmp_" + tgt_table_name)
    except FileNotFoundError:
        print("The temp table does not exist.")
    final_data_df.write.mode("overwrite").saveAsTable("tmp_" + tgt_table_name)
    tmp_data_tbl = spark.table("tmp_" + tgt_table_name)
    tmp_data_tbl.write.mode("overwrite").insertInto(tgt_table_name)

    spark.sql("SELECT * FROM {} ORDER BY 1,2".format(tgt_table_name)).show()


if __name__ == '__main__':

    warehouse_location = abspath('/home/ubuntu/workspace/spark-warehouse/')

    spark = SparkSession.builder\
        .appName("ETL Place Details")\
        .master("local[*]")\
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    path = "/home/ubuntu/workspace/data/"

    place_df = spark.read.json(path + "placeDetails.json")
    populate_exploding_dims(
        place_df, "place_payment_methods", "placeId", "acceptedPayments")
    populate_exploding_dims(place_df, "place_cuisines",
                            "placeId", "servedCuisines")
    populate_exploding_dims(place_df, "place_parking",
                            "placeId", "parkingType")
