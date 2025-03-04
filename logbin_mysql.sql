CREATE DATABASE nieuwe_database
    DEFAULT CHARACTER SET = 'utf8mb4';

USE nieuwe_database;

CREATE TABLE Orders ( 
    OrderId int, 
    OrderStatus varchar(30), 
    LastUpdated 
    timestamp
 ); 

INSERT INTO Orders 
VALUES(1,'Backordered', '2020-06-01 12:00:00');

INSERT INTO Orders 
VALUES(1,'Shipped', '2020-06-09 12:00:25');

INSERT INTO Orders 
VALUES(2,'Shipped', '2020-07-11 3:05:00');

INSERT INTO Orders 
VALUES(1,'Shipped', '2020-06-09 11:50:00');

INSERT INTO Orders 
VALUES(3,'Shipped', '2020-07-12 12:00:00');

SHOW VARIABLES LIKE '%log_bin%';        --log_bin should be "ON"

SHOW VARIABLES LIKE 'binlog_format';    --binlog_format should be "ROW"
