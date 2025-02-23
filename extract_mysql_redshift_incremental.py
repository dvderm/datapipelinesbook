# Extracting data from an AWS RDS MySQL database, comparing it to the most recent value from a LastUpdated column from a Redshift Serverless table, only extract the rows from the MySQL table that are more recent than the date retrieved from Redshift...
# ... saving those rows to a local file in CSV format and uploading the CSV to S3

import pymysql
import csv
import boto3
import configparser
import psycopg2 
import os

# get db Redshift connection info
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port") 
 
# connect to the redshift cluster
rs_conn = psycopg2.connect( 
    "dbname=" + dbname 
    + " user=" + user 
    + " password=" + password 
    + " host=" + host 
    + " port=" + port) 
 
rs_sql = """SELECT COALESCE(MAX(LastUpdated),
        '1900-01-01')
        FROM Orders;"""
rs_cursor = rs_conn.cursor()
rs_cursor.execute(rs_sql)
result = rs_cursor.fetchone() 
 
# there's only one row and column returned with the MAX of the column LastUpdated
last_updated_warehouse = result[0] 

print(f"Most recent value in LastUpdated column: {last_updated_warehouse}")
 
rs_cursor.close()
rs_conn.commit() 
 
# get the MySQL connection info and connect
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
  print("MySQL connection established!") 

# Get all rows from Orders table in MySQL database and save it locally as an CSV file
m_query = """SELECT *
      FROM Orders
      WHERE LastUpdated > %s;"""
local_filename = "order_extract.csv" 
 
m_cursor = conn.cursor()
m_cursor.execute(m_query, (last_updated_warehouse,))
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