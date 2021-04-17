# Overview

The ETL pipline consists of a Amazon s3 bucket which serves as the source location for the input files. An airflow job scheduled to run every hour downloads the input files from the source server and feeds it to apache spark job which can be run on an EMR cluster in order to extract, transform and load fact and dimension tables in Hive stored in parquet file format.

