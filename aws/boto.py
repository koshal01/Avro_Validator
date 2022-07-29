import os
import boto3
import pandas as pd

sts_client = boto3.client('sts')

role_obj = sts_client.assume_role(RoleArn="arn:aws:iam::718515174980:role/HDP-DATA-PANORAMA-POWERUSER", RoleSessionName='tests3buckets')

credentials_temp = role_obj['Credentials']

aws_access_key = credentials_temp['AccessKeyId']
aws_secret_key = credentials_temp['SecretAccessKey']
aws_session_token = credentials_temp['SessionToken']

s3client = boto3.client('s3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    aws_session_token=aws_session_token
)

#bucket_name = "dl-dev-data-panorama-etl-pan-syed-718515174980"
bucket_name = "dl-dev-data-panorama-sagar-panorama-718515174980"
S3_PREFIX = "victoria"
# s3_object = "victoria/SYSTEM_COST_AND_LOCATION.delta/part-00000-123f669b-f060-43a7-b789-45ed5250445e-c000.snappy.parquet"

response = s3client.list_objects_v2(Bucket=bucket_name, Prefix=S3_PREFIX)
s3_objects = [obj["Key"] for obj in response["Contents"]]


print("start download")
cnt = 23
for s3_file in s3_objects:
    local_file_path = os.path.join("/home/chiragt/koshal/", s3_file.replace(S3_PREFIX, ""))
    parts = s3_file.split('.')
  
    if "snappy" in parts:
      filearray = local_file_path.split('/')
      str_cnt= str(cnt)
      filename = filearray[1] + str_cnt + ".snappy.parquet"
      cnt += 1
      s3client.download_file(bucket_name, s3_file, filename)
      
print("complete download")
