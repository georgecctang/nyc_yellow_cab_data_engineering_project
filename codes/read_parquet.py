#Objective: Load parquet files from S3 into pandas dataframe in local drive
#Created by: George Tang

import boto3


import boto3
import configparser
import io
import pyarrow.parquet as pq
import pandas as pd
import s3fs

#obtain AWS credentials
config = configparser.ConfigParser()
config.read_file(open('credentials.cfg'))
AWS_KEY_ID = config.get("AWS","KEY")
AWS_SECRET = config.get("AWS","SECRET")
    
client = boto3.resource('s3', \
                        region_name='us-west-2', \
                        aws_access_key_id=AWS_KEY_ID, \
                        aws_secret_access_key=AWS_SECRET)

#project bucket name
bucketName = 'nyc-yellow-cab-project'
#prefix for all parquets for 2019/01
prefix = 'analytics/year=2019/month=1'

#obtain list of object meta from bucket
response = s3.list_objects(Bucket=bucketName, Prefix=prefix)

#create list of parquet keys
parquet_list = [item['Key'] for item in response['Contents'] if item['Key'].endswith('.parquet')]

df_list = []

#load parquet in df and store df in list 
for filename in parquet_list:

    buffer = io.BytesIO()
    obj = client.Object(bucketName, filename)
    obj.download_fileobj(buffer)

    df = pd.read_parquet(buffer)
    df_list.append(df)

#Create final dataframe
df = pd.concat(df_list,ignore_index=True)



