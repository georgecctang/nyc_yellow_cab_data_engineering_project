#Objective: Create project S3 bucket
#Created by: George Tang

import boto3
import configparser


def main():
	
	config = configparser.ConfigParser()
	config.read_file(open('credentials.cfg'))
	AWS_KEY_ID=config.get("AWS","KEY")
	AWS_SECRET=config.get("AWS","SECRET")
	
	bucket_name = 'nyc-yellow-cab-project'
	region = 'us-west-2'
	
	s3 = boto3.client('s3', region_name=region, \
		aws_access_key_id=AWS_KEY_ID,
		aws_secret_access_key=AWS_SECRET)
		
	bucket = s3.create_bucket(Bucket=bucket_name,
		CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'})

if __name__ == "__main__":
    main()