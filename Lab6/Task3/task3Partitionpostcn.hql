--Alter the manual table to add the partitions.
USE ${hiveconf:databaseName};
ALTER TABLE RoseDynamicEmployeesManualAdd ADD PARTITION (dept='csse');
ALTER TABLE RoseDynamicEmployeesManualAdd ADD PARTITION (dept='admin');
ALTER TABLE RoseDynamicEmployeesManualAdd ADD PARTITION (dept='ece');
