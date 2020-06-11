# New York City Yellow Cab Data Engineering Project 
## Introduction
The iconic Yellow Cab is one of the many for hire vehicles in New York City, with its fleet providing over 7 million trips every month. Any city dweller would know too well that no two taxi trips are alike, even with the same pickup and dropoff locations. For instance, a trip during rush hour on a snowy day would likely take longer and cost more than one during a sunny morning in the weekend. Understanding factors that affect ride duration and fare would allow Yellow Cab to provide better service. That, in turn, can increase customer satistification and ultimately revenuve, in terms of both usage and service tips. 
 
## Scope
The scope of this project is to create an ETL pipeline that process and combine trip record and weather data, and create datasets ready for use for analytics.  Data scientists and analysts can then use this dataset to identify factors that affect ride duration and fare, and develop recommendations for service improvement. 

## Repository Structure
The ```codes``` file contains the scripts used in this project.  
The ```data``` file contains some of the data files used in this project.  
The ```image``` file contains the image files.  

## Source Data
### Trip Records Data
The trip records data is obtained from [NYC Taxi & Limousine Commission's page](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Trip data are stored by month in csv format. The data for 2019, the most recent year with available data, would be used for this project. The total number of records is over 80 million. The data are stored in S3 bucket    ```nyc-tlc```. They are transferred to the project bucket  ```nyc-yellow-cab-project```.  The data dictionary for the dataset is provided [here](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
### Taxicode
The pickup and dropoff locations for each trip is recorded as Location IDs, which are identified in this [lookup table](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv).

### Vendor, Payment, Rate Tables
Vendor, payment and rate tables contain the reference codes and names for the VendorID, Payment Type and RatecodeID in the trip records table, respectively. Those tables are created locally based on the [data dictionary](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) and uploaded to the project bucket.

### Weather Data
Weather data of NYC for the year of 2019 in json format is obtained from [OpenWeatherMap]([https://home.openweathermap.org/history_bulks/new](https://home.openweathermap.org/history_bulks/new)). It is first downloaded to the local drive and then uploaded to the project bucket. The data dictionary for the weather data is provided [here]([https://openweathermap.org/weather-data](https://openweathermap.org/weather-data)).

## Tools
This project consists using the following tools:
```Amazon S3``` For the storage of source data and output data
```Amazon EMR``` For this project, the data lake approach is chosen over a more traditional data warehouse approach. Given the ultimate goal is to support data analytics, it is possible that additional data, structured or unstructured, will be added in future. Data lake on EMR clusters would provide tremendous flexibility in storing and processing unstructured data (e.g. text data from traffic accident reports).

## Data Model

![alt text](https://github.com/georgecctang/nyc_yellow_cab_data_engineering_project/blob/master/image/schema.png "Data Model")

The data model consists of 2 fact tables and 5 dimension tables.
The trip records fact table connects to the 5 dimension tables, namely vendors, rates, payments, taxizones, and time. 
The weather fact table is standalone.  
The two fact tables are connect through the time table.

## Exploratory Data Analysis
The purpose of EDA  is to identify data quality issues. See the file ```/eda.ipynb``` for the detailed descriptions.  
Overall, the trip records dataset is relatively clean with a only few minor issues:
1. Some records have VendorID not in the provided data dictionary. *Solution: Modify ```vendor.json``` to include those VendorID.*
2. Some records have RatecodeID not in the provided data dictionary. *Solution: Modify ```rate.json``` to include those RatecodeID.*
3. Some records have out-of-range pickup datetime, e.g. February records in the January dataset. *Solution: Filter out records with out-of-range datetime during data processing in EMR cluster.*
4. Some records have missing VendorID. *Solution: Filter out records with missing VendorID during data processing in EMR cluster.*

The weather data does not have any data issues. 
## Process
This project consists of  the following steps:  
1. Setup project buckets  
	i. run ```create_s3_bucket.py``` to create project bucket named ```nyc-yellow-cab-project```  
	ii.run ```copy_s3_to_s3.py``` to copy trips record data and taxizone data to project bucket.  
	iii. run ```upload_local_to_s3.py``` to upload weather data, vendor, payment and rate data to project bucket.  
2. Setup EMR Cluster  
I performed this step in the AWS console with the following settings:  
		- Instance Type: m5.xlarge  
		- Number of instances: 3  
3. ETL  
	Run the script ```etl.ipynb``` in the Jupyter Lab attached to the cluster. The ETL pipeline consists of the following steps:  
	i. Load and modify weather table  
	ii. Load and modify trip records table  
	iii. Load taxicode, vendor, rate and payment table  
	iv. Create time table  
	v. Create analytics table by joining trips, time and weather tables  
	vi. Perform two data quality checks: (a) The number of null values in taxi provider (expected result: 0), (b) number of null values in temperature (expected result: 0)  
	vii. Upload the analytics table to the project bucket as parquet files. The key is ```analytics``` and the files are partitioned by year and month.  
4. Read parquet file (Optional)  
Use the code in ```read_parquet.py``` to load the parquet files into local drive for data analytics.

## Data Dictionary
The data dictionary for the analytics table is included in this repository. 
## Scenario Analysis
This section dissuss a few scenarios that we may encounter in the future, and offer potential solutions.  
**The data was increased by 100x.**  
To account for this, we can use a more powerful instance type and increase the number of instances in the EMR cluster.  
**The pipelines would be run on a daily basis by 7 am every day.**  
 We can set up an automated ETL process pipeline in airflow. The steps would include:  
	 - Copy data from NYC's S3 bucket to project S3 bucket  
	 - Inititate EMR cluster  
	 - Load data from S3 to EMR cluster
	 - Process data in EMR   
	 - Perform quality check  
	 - Load processed data tables to S3  
**The database needed to be accessed by 100+ people**  
For this particular application, only the data engineering team would need to process the data with EMR. The final analytics dataset for analysis will be stored in the project S3 bucket for access.  
S3 is designed for frequent access by multiple users. If the data will be accessed by users from another region (e.g. Australia), one option for reducing latency is to replicate the data to a bucket in that region. It can be automated with S3's cross region replication (CRR) feature, where files uploaded to the project bucket will be automatically replicated to the replica bucket. 
