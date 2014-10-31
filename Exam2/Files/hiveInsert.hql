USE ${hiveconf:databaseName};

LOAD DATA INPATH "${hiveconf:pigOutputLocation}" OVERWRITE INTO TABLE ${hiveconf:tempTableName} PARTITION(username='postcn');
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hiveconf:tableName} PARTITION(username) SELECT * FROM ${hiveconf:tempTableName};
