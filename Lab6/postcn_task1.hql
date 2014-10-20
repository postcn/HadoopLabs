CREATE DATABASE IF NOT EXISTS ${hiveconf:databaseName};
USE ${hiveconf:databaseName};
CREATE TABLE IF NOT EXISTS ${hiveconf:tableName}(year int, temp int, quality int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
LOAD DATA INPATH "${hiveconf:inputLocation}" OVERWRITE INTO TABLE ${hiveconf:tableName};

--Now do the data work
SELECT year, AVG(temp), MIN(temp), MAX(temp) FROM ${hiveconf:tableName} WHERE quality=0 OR quality = 1 GROUP BY year;
