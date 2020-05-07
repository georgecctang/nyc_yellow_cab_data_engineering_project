#Objective: Move files from local folder to project s3 bucket
#Created by: George Tang

import boto3
import configparser


def main():

	file_path = '../data/'
	des_bucket = 'nyc-yellow-cab-project'


	#populate code table upload list
	code_tables = ['payment.json','ratecode.json','vendor.json'] 

	#create the code tables upload list for input to AWS client upload_file method
	#item consists of (local file path,destination bucket, destination key)
	upload_list = [(file_path + table, des_bucket, 'codes/' + table) for table in code_tables]
	
    #obtain AWS credentials
	config = configparser.ConfigParser()
	config.read_file(open('credentials.cfg'))
	AWS_KEY_ID=config.get("AWS","KEY")
	AWS_SECRET=config.get("AWS","SECRET")
    
    #create AWS client for file upload from local drive to project s3
	client = boto3.client('s3', \
	region_name = 'us-west-2', \
	aws_access_key_id=AWS_KEY_ID, \
	aws_secret_access_key=AWS_SECRET)
    
	for filepath, des_bucket, des_key in upload_list:
		client.upload_file(filepath, des_bucket, des_key)

if __name__ == "__main__":
    main()
            