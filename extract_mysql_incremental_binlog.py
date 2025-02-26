# Extra documentation in order to get the binlog set up in AWS RDS MySQL: create MySQL database > make sure automated backups are turned on and backup retention period is at least 1 day > 
# create parameter group > change parameter values binlog_format to "ROW", binlog_row_image to "full", binlog_row_metadata to "FULL" > go to database > click "Modify" > add parameter group to database >
# check if changes have been made to parameters > SHOW VARIABLES LIKE '%log_bin%'; key "log_bin" should have value "ON" > SHOW VARIABLES LIKE 'binlog_format'; key "binlog_format" should be "ROW" >
# make changes to your database: INSERT values and then run the script below

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication

# get the MySQL connection info
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password") 
 
mysql_settings = { 
    "host": hostname, 
    "port": int(port), 
    "user": username, 
    "passwd": password
 } 

# Connect to MySQL database
b_stream = BinLogStreamReader( 
            connection_settings = mysql_settings, 
            server_id=100, 
            only_events=[row_event.DeleteRowsEvent, 
                        row_event.WriteRowsEvent, 
                        row_event.UpdateRowsEvent] 
            ) 

for event in b_stream: 
    event.dump() 
 
b_stream.close()