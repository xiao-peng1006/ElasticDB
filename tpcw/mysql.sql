GRANT ALL ON *.* TO 'root'@'%' IDENTIFIED BY 'TigerBit!2016';
FLUSH PRIVILEGES;

drop database if exists tpcw;
purge binary logs before now();
create database tpcw;

drop database if exists canvasjs_db;
create database canvasjs_db;
use canvasjs_db;
create table datapoints (x double, u double, ur double, uw double, r double, w double, m double);
insert into datapoints values
(0,1,1,1,1,3,4),(1,2,1,1,4,27,2),(2,3,1,1,9,81,1.4),(3,4,1,1,16,243,1.2),(4,5,1,1,25,600,1.1);

