#Objective: Move files from source s3 bucket to project s3 bucket
#Created by: George Tang

import boto3
import configparser

def main():
    
	src_bucket = 'nyc-tlc'
	des_bucket = 'nyc-yellow-cab-project'

	#get data for year 2019
	years = range(2019,2020)
	months = [f'{i:02d}' for i in range(1,13)]

	#the list consists of tuples of (source bucket,source key, destination bucket,  destination key)
	#bucket key list for the trip data
	copy_list = [(src_bucket, 'trip data/yellow_tripdata_' + f'{year}-{month}.csv', \
					des_bucket, 'tripdata/' + f'{year}/{month}.csv') for year in years for month in months]

	#append for taxi zone data
	copy_list.append((src_bucket, 'misc/taxi _zone_lookup.csv',des_bucket,'codes/taxizone.csv'))
	
	
    #obtain AWS credentials
	config = configparser.ConfigParser()
	config.read_file(open('credentials.cfg'))
	AWS_KEY_ID=config.get("AWS","KEY")
	AWS_SECRET=config.get("AWS","SECRET")
    
    #create AWS resource for file copy from source s3 to project s3
	s3 = boto3.resource('s3', \
	aws_access_key_id=AWS_KEY_ID, \
	aws_secret_access_key=AWS_SECRET)
    
	for src_bucket, src_key, des_bucket, des_key in copy_list:
		copy_source = {'Bucket': src_bucket, 'Key': src_key}
		s3.meta.client.copy(copy_source, des_bucket, des_key)

if __name__ == "__main__":
	main()  