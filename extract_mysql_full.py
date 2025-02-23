# Extracting data from an AWS RDS MySQL database, storing it locally as a CSV and uploading it to S3

import pymysql
import csv
import boto3
import configparser
import os

# Create connection to mysql database
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password") 

conn = pymysql.connect(host=hostname, 
        user=username, 
        password=password, 
        db=dbname, 
        port=int(port))
 
if conn is None: 
  print("Error connecting to the MySQL database")
else: 
  print(f"MySQL connection established! Connection object created: {conn}")

# Extracting data from orders table and store it locally in a CSV file
m_query = "SELECT * FROM Orders;"
local_filename = "order_extract.csv" 

m_cursor = conn.cursor()    # Creates cursor object which is used to execute SQL queries
m_cursor.execute(m_query)
results = m_cursor.fetchall() 

with open(local_filename, 'w') as fp: 
    csv_w = csv.writer(fp, delimiter='|') 
    csv_w.writerows(results)
    print(f"File written to path: {os.path.abspath(local_filename)}")
    
fp.close()
m_cursor.close()
conn.close()

# Create connection to S3 bucket and upload local CSV
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client('s3', 
   aws_access_key_id=access_key,
   aws_secret_access_key=secret_key) 

s3_file = local_filename 
s3.upload_file(local_filename, bucket_name, s3_file)