#!/bin/bash
#mysql path: /hadoop04.csse.rose-hulman.edu:3306
if [ "$2" = "1" ]
then
	sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --split-by eid --target-dir /tmp/SqoopOutput
elif [ "$2" = "2" ]
then
	sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --split-by eid --target-dir /tmp/SqoopMapOutput -m 1
elif [ "$2" = "3" ]
then
	sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --split-by eid --target-dir /tmp/SqoopSeqOutput -m 2 --as-sequencefile
elif [ "$2" = "4" ]
then
	sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --split-by eid --warehouse-dir /tmp/sqoop -m 2 
elif [ "$2" = "5" ]
then
	sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --split-by eid --warehouse-dir /tmp/sqoop -m 2 --null-string 'This is a Null String' --fields-terminated-by '\t'
elif [ "$2" = "7" ]
then
	sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --split-by eid  -m 2 --hive-import --create-hive-table --hive-table sqooptest.Employees
elif [ "$2" = "8" ]
then
	sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --split-by eid  -m 2 --hive-import --create-hive-table --hive-table sqooptest.Employees --null-string 'This is a Null String' --fields-terminated-by '\t'
elif [ "$2" = "9" ]
then
	sqoop import-all-tables --connect jdbc:mysql://$1/sqooptest --username root --warehouse-dir /tmp/sqoopAll -m 1
else
	echo "Usage: task3postcn.sh <database server> <task to run>"
	echo "Acceptable tasks to run are 1-9"
fi

