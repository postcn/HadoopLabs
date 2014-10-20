CREATE DATABASE IF NOT EXISTS ${hiveconf:databaseName};
USE ${hiveconf:databaseName};

CREATE TABLE IF NOT EXISTS RoseEmployees(firstName string, lastName string, speciality string, dept string, eNum int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA INPATH "${hiveconf:allEmployeesLocation}" OVERWRITE INTO TABLE RoseEmployees;

CREATE TABLE IF NOT EXISTS RoseStaticEmployees(firstName string, lastName string, speciality string, eNum int) PARTITIONED BY (dept string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
TRUNCATE TABLE RoseStaticEmployees;
LOAD DATA INPATH "${hiveconf:csseEmployeesLocation}" INTO TABLE RoseStaticEmployees Partition(dept = 'csse');
LOAD DATA INPATH "${hiveconf:eceEmployeesLocation}" INTO TABLE RoseStaticEmployees Partition(dept = 'ece');
LOAD DATA INPATH "${hiveconf:adminEmployeesLocation}" INTO TABLE RoseStaticEmployees Partition(dept = 'admin');

--RoseDynamicEmployees

CREATE TABLE IF NOT EXISTS RoseDynamicEmployees(firstName string, lastName string, specialty string, eNum int) PARTITIONED BY (dept string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS orc;
TRUNCATE TABLE RoseDynamicEmployees;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE RoseDynamicEmployees PARTITION(dept) SELECT firstName, lastName, speciality, enum, dept FROM RoseStaticEmployees;

--RoseStaticEmployeesORC

CREATE TABLE IF NOT EXISTS RoseStaticEmployeesORC(firstName string, lastName string, speciality string, enum int) PARTITIONED BY (dept string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS orc;
TRUNCATE TABLE RoseStaticEmployeesORC;
SET hive.exec.dynamic.partition.mode=strict;
INSERT INTO TABLE RoseStaticEmployeesORC PARTITION(dept='csse') SELECT firstName, lastName, speciality, enum FROM RoseEmployees WHERE dept='csse';
INSERT INTO TABLE RoseStaticEmployeesORC PARTITION(dept='admin') SELECT firstName, lastName, speciality, enum FROM RoseEmployees WHERE dept='admin';
INSERT INTO TABLE RoseStaticEmployeesORC PARTITION(dept='ece') SELECT firstName, lastName, speciality, enum FROM RoseEmployees WHERE dept='ece';

--Count Each Table
CREATE TABLE IF NOT EXISTS TempTableORC(tableName string, recordCount int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS orc;
INSERT INTO TABLE TempTableORC SELECT 'RoseStaticEmployeesORC', COUNT(*) FROM RoseStaticEmployeesORC;
INSERT INTO TABLE TempTableORC SELECT 'RoseEmployees', COUNT(*) FROM RoseEmployees;
INSERT INTO TABLE TempTableORC SELECT 'RoseDynamicEmployees', COUNT(*) FROM RoseDynamicEmployees;
INSERT INTO TABLE TempTableORC SELECT 'RoseStaticEmployees', COUNT(*) FROM RoseStaticEmployees;

SELECT * FROM TempTableORC;
TRUNCATE TABLE TempTableORC;
DROP TABLE TempTableORC;

--Count Partitions
CREATE TABLE IF NOT EXISTS TempPartCountTableORC(tableName string, partitionCount int) ROW FORMAT DELIMITED FIELDS TERMINATED BY',' STORED as orc;
INSERT INTO TABLE TempPartCountTableORC SELECT 'RoseStaticEmployeesORC', COUNT(DISTINCT dept) FROM RoseStaticEmployeesORC;
INSERT INTO TABLE TempPartCountTableORC SELECT 'RoseDynamicEmployees', COUNT(DISTINCT dept) FROM RoseDynamicEmployees;
INSERT INTO TABLE TempPartCountTableORC SELECT 'RoseStaticEmployees', COUNT(DISTINCT dept) FROM RoseStaticEmployees;

SELECT * FROM TempPartCountTableORC;
TRUNCATE TABLE TempPartCountTableORC;
DROP TABLE TempPartCountTableORC;

--Manually Adding Table
CREATE TABLE IF NOT EXISTS RoseDynamicEmployeesManualAdd(firstName string, lastName string, speciality string, enum int) PARTITIONED BY (dept string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS orc;

