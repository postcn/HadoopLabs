REGISTER '$jar';
DEFINE Quality edu.rosehulman.postcn.IsGoodQuality();
records = LOAD '$input' using PigStorage('\t') AS (year:int, temperature:int, quality:int);
frecords = FILTER records by Quality(quality);
grecords = GROUP frecords by year;
temp = FOREACH grecords GENERATE group, MIN(frecords.temperature) AS MinTemp, MAX(frecords.temperature) AS MaxTemp, AVG(frecords.temperature) AS AvgTemp;
DUMP temp
STORE temp into '$output' USING PigStorage(',');
