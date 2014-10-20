CREATE DATABASE IF NOT EXISTS ${hiveconf:databaseName};
USE ${hiveconf:databaseName};

CREATE TABLE IF NOT EXISTS archiveLogData(blog string, hit double, error double, year int, month int, day int, hour int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

LOAD DATA INPATH "${hiveconf:pigOutputDir}${hiveconf:jobDate}" INTO TABLE archiveLogData;

CREATE TABLE IF NOT EXISTS logData(blog string, hit double, error double) PARTITIONED BY (year int, month int, day int, hour int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS orc;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE logData Partition(year, month, day, hour) SELECT blog, hit, error, year, month, day, hour FROM archiveLogData WHERE year=${hiveconf:year} AND month=${hiveconf:month} AND day=${hiveconf:day} AND hour =${hiveconf:hour};

CREATE TABLE IF NOT EXISTS logCompare(table string, count int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS orc;
TRUNCATE TABLE logCompare;

INSERT INTO TABLE logCompare SELECT 'archiveLogData', COUNT(*) FROM archiveLogData;
INSERT INTO TABLE logCompare SELECT 'logData', COUNT(*) FROM logData;

SELECT * FROM logCompare;
TRUNCATE TABLE logCompare;
DROP TABLE logCompare;
