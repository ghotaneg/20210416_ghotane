import os
from os.path import abspath
import re
import shutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func
from datetime import datetime


def populate_user_dim(dataframe, tgt_table_name, key_col_name):
    dim_col_list = ['userId', 'activity', 'ambience', 'budget', 'dress_preference', 'drink_level', 'hijos', 'interest', 'marital_status', 'personality',
                    'religion', 'smoker', 'transport', 'birth_year', 'weight', 'height', 'latitude', 'longitude']

    tmp_tgt_table = "tmp_" + tgt_table_name

    new_data_df = user_df[[dim_col_list]]

    existing_df = spark.sql("SELECT * FROM {}".format(tgt_table_name))
    old_data_df = existing_df.join(new_data_df, existing_df[key_col_name] == new_data_df[key_col_name], 'left').where(
        new_data_df[key_col_name].isNull()).select(existing_df['*'])
    final_data_df = old_data_df.union(new_data_df)

    try:
        shutil.rmtree(os.path.join(warehouse_location, tmp_tgt_table))
    except FileNotFoundError:
        print("The temp table '{}' does not exist.".format(tmp_tgt_table))

    final_data_df.write.mode("overwrite").saveAsTable(tmp_tgt_table)
    tmp_data_tbl = spark.table(tmp_tgt_table)
    tmp_data_tbl.write.mode("overwrite").insertInto(tgt_table_name)


def populate_exploding_dims(dataframe, tgt_table_name, key_col_name, exp_col_name):
    df = dataframe[[key_col_name, exp_col_name]]\
        .withColumn(exp_col_name, func.explode(exp_col_name))\
        .withColumn(exp_col_name, func.lower(func.regexp_replace(exp_col_name, '-', '_')))

    df_nulls = dataframe.select([key_col_name, exp_col_name]).where(
        '{} IS NULL'.format(exp_col_name))
    df_nulls = df_nulls.withColumn(exp_col_name, func.lit('unknown'))
    new_data_df = df.union(df_nulls)
    # new_data_df = new_data_df\
    #     .withColumn(key_col_name, new_data_df[key_col_name].cast(StringType()))
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


def remove_special_chars(col_name):
    c1 = func.regexp_replace(col_name, "[\?]", "unknown")
    return func.lower(func.regexp_replace(c1, "[\s-]", "_"))


def extract(src_path):
    df = spark.read.json(src_path)

    string_col_list = ['activity', 'ambience', 'budget', 'dress_preference', 'drink_level',
                       'hijos', 'interest', 'marital_status', 'personality', 'religion', 'smoker', 'transport']
    int_col_list = ['birth_year', 'weight']
    float_col_list = ['height', 'latitude', 'longitude']

    for col_name in string_col_list:
        df = df.withColumn(col_name, remove_special_chars(col_name))

    for col_name in int_col_list:
        df = df.withColumn(col_name, func.col(col_name).cast(IntegerType()))

    for col_name in float_col_list:
        df = df.withColumn(col_name, func.col(col_name).cast(FloatType()))

    return df


def populate_fct_interaction_details(user_place_interaction_df, tgt_table_name):

    upi_df = user_place_interaction_df.withColumn(
        'placeInteractionDetails', func.explode('placeInteractionDetails'))

    upi_df = upi_df\
        .withColumn('year', func.substring('placeInteractionDetails.visitDate', 1, 4))\
        .withColumn('month', func.substring('placeInteractionDetails.visitDate', 6, 2))\
        .withColumn('day', func.substring('placeInteractionDetails.visitDate', 9, 2))\
        .withColumn('insertedDateTime', func.lit(datetime.now()))

    upi_df = upi_df.select(
        upi_df['userId'],
        upi_df['placeInteractionDetails.placeID'].cast(IntegerType()),
        upi_df['placeInteractionDetails.foodRating'].cast(IntegerType()),
        upi_df['placeInteractionDetails.restRating'].cast(IntegerType()),
        upi_df['placeInteractionDetails.serviceRating'].cast(IntegerType()),
        upi_df['placeInteractionDetails.salesAmount'].cast(DoubleType()),
        upi_df['placeInteractionDetails.visitDate'].cast(DateType()),
        upi_df['placeInteractionDetails.visitDate'].cast(
            TimestampType()).alias('visitDateTime'),
        upi_df['insertedDateTime'].cast(TimestampType()),
        upi_df['year'].cast(IntegerType()),
        upi_df['month'].cast(IntegerType()),
        upi_df['day'].cast(IntegerType())
    )

    upi_df = upi_df.dropDuplicates(['userId', 'placeId', 'visitDate'])

    existing_df = spark.sql("SELECT * FROM {}".format(tgt_table_name))
    e_df = existing_df.alias('e_df')
    n_df = upi_df.alias('n_df')

    dedup_df = n_df.join(e_df,
                         (n_df['userId'] == e_df['userId']) & (n_df['placeID'] == e_df['placeID']) & (
                             n_df['visitDate'] == e_df['visitDate']),
                         'left').where(e_df.userId.isNull()).select(n_df['*'])

    dedup_df.write.mode("append").insertInto(tgt_table_name)


if __name__ == '__main__':

    warehouse_location = abspath('/home/ubuntu/workspace/spark-warehouse/')

    spark = SparkSession.builder\
        .appName("ETL User Details")\
        .master("local[*]")\
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    src_path = "/home/ubuntu/workspace/data/userDetails.json"

    user_df = extract(src_path)
    populate_user_dim(user_df, "users", "userId")
    populate_exploding_dims(user_df, "user_payment_methods",
                            "userId", "userPaymentMethods")
    populate_exploding_dims(user_df, "user_cuisines", "userId", "favCuisine")
    populate_fct_interaction_details(
        user_df[['userId', 'placeInteractionDetails']], "fct_user_place_interaction")
