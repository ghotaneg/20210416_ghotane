from os.path import abspath
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.types
import pyspark.sql.functions as func

warehouse_location = abspath('/home/ubuntu/workspace/spark-warehouse')

spark = SparkSession.builder\
    .appName("Initial Data Load")\
    .master("local[*]")\
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

spark.sql("DROP TABLE IF EXISTS place_payment_methods")
spark.sql("CREATE TABLE IF NOT EXISTS place_payment_methods (placeId INT, acceptedPayments STRING) STORED AS PARQUET")

spark.sql("DROP TABLE IF EXISTS place_cuisines")
spark.sql("CREATE TABLE IF NOT EXISTS place_cuisines (placeId INT, servedCuisines STRING) STORED AS PARQUET")

spark.sql("DROP TABLE IF EXISTS place_parking")
spark.sql("CREATE TABLE IF NOT EXISTS place_parking (placeId INT, parkingType STRING) STORED AS PARQUET")

spark.sql("DROP TABLE IF EXISTS user_payment_methods")
spark.sql("CREATE TABLE IF NOT EXISTS user_payment_methods (userId STRING, userPaymentMethods STRING) STORED AS PARQUET")

spark.sql("DROP TABLE IF EXISTS user_cuisines")
spark.sql("CREATE TABLE IF NOT EXISTS user_cuisines (userId STRING, favCuisine STRING) STORED AS PARQUET")

spark.sql("DROP TABLE IF EXISTS users")
spark.sql("""CREATE TABLE IF NOT EXISTS users (userId STRING, activity STRING, ambience STRING, budget STRING, dress_preference STRING, 
    drink_level STRING, hijos STRING, interest STRING, marital_status STRING, personality STRING, religion STRING, smoker STRING, 
    transport STRING, birth_year INT, weight INT, height INT, latitude FLOAT, longitude FLOAT) STORED AS PARQUET""")

spark.sql("DROP TABLE IF EXISTS fct_user_place_interaction")
spark.sql("""CREATE TABLE IF NOT EXISTS fct_user_place_interaction (userId STRING, placeID INT, foodRating INT, restRating INT, 
    serviceRating INT, salesAmount DOUBLE, visitDate DATE, visitDateTime TIMESTAMP, insertedDateTime TIMESTAMP) 
    PARTITIONED BY (year int, month int, day int) STORED AS PARQUET""")
