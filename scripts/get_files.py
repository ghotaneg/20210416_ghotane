import os
import sys
import datetime
import boto3
import botocore
from configparser import ConfigParser


def download_files_from_s3(bucket_name, tgt_dir):

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects = bucket.objects.filter(Prefix='pending/')

    for obj in objects:
        path, filename = os.path.split(obj.key)
        if filename:
            bucket.download_file(obj.key, tgt_dir + filename)


if __name__ == "__main__":
    config = ConfigParser()
    config.read("/home/ubuntu/workspace/configs/config.ini")

    BUCKET_NAME = config['INFO']['BUCKET_NAME']
    TGT_DIR = config['INFO']['DATA_DIR']

    download_files_from_s3(BUCKET_NAME, TGT_DIR)
