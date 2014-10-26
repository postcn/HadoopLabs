#!/bin/bash
#mysql path: /hadoop04.csse.rose-hulman.edu:3306
if [ "$2" = "1" ]
then
	sqoop export --connect jdbc:mysql://$1/sqooptest --username root --table EmployeesExportData -m 1 --input-null-string 'This is a Null String' --export-dir /tmp/sqoop/Employees
elif [ "$2" = "2" ]
then
	sqoop export --connect jdbc:mysql://$1/sqooptest --username root --table EmployeesExportData -m 1 --input-null-string 'This is a Null String' --export-dir /tmp/sqoop/Employees --update-key eid --update-mode allowinsert
else
	echo "Usage: task4postcn.sh <database server> <task to run>"
	echo "Acceptable tasks to run is 1 or 2"
fi
