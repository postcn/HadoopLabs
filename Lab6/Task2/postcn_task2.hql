CREATE DATABASE IF NOT EXISTS ${hiveconf:databaseName};
USE ${hiveconf:databaseName};

CREATE TABLE IF NOT EXISTS ${hiveconf:tableName}(text string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n' STORED AS TEXTFILE;
LOAD DATA INPATH "${hiveconf:inputLocation}" OVERWRITE INTO TABLE ${hiveconf:tableName};

--Load UDF
DROP FUNCTION IF EXISTS UdfUpper;
DROP FUNCTION IF EXISTS UdfStrip;

CREATE FUNCTION UdfUpper AS 'edu.rosehulman.postcn.Upper' USING JAR 'hdfs:///user/root/postcn_Lab6/Lab6UDF-0.0.1-SNAPSHOT.jar';
CREATE FUNCTION UdfStrip AS 'edu.rosehulman.postcn.Strip' USING JAR 'hdfs:///user/root/postcn_Lab6/Lab6UDF-0.0.1-SNAPSHOT.jar';

--Now execute word count
SELECT word, COUNT(*) FROM ${hiveconf:tableName} LATERAL VIEW explode(split(UdfUpper(UdfStrip(text)), ' ')) lTable as word GROUP BY word;
