REGISTER '$jar';
DEFINE Ratio edu.rosehulman.postcn.RatioCalc();
%declare DATE `date +"%Y-%m-%d"`;
records = LOAD '$input' using PigStorage('\t') AS (date:chararray, time:chararray, xEdgeLocation:chararray, scBytes:int, cIp:chararray, csMethod:chararray, csHost:chararray, csUriStem:chararray, scStatus:int, csReferer:chararray, csUserAgent:chararray, csUriQuery:chararray, csCookie:chararray, xEdgeResultType:chararray, xEdgeRequestId:chararray, xHostHeader:chararray, csProtocol:chararray, csBytes:int, timeTaken:int);
cleanedRecords = FOREACH records GENERATE FLATTEN(STRSPLIT(csUriStem, '/', 4)) AS (empty, blogPlaceHolder, name, extra), xEdgeResultType;
selectedFromCleanedRecords = FOREACH cleanedRecords GENERATE name, xEdgeResultType;

filteredHits = FILTER selectedFromCleanedRecords BY xEdgeResultType MATCHES 'Hit';
filteredMiss = FILTER selectedFromCleanedRecords BY xEdgeResultType MATCHES 'Error';
groups = COGROUP selectedFromCleanedRecords BY name, filteredHits BY name, filteredMiss BY name;

selected = FOREACH groups GENERATE group AS blog, Ratio(COUNT(filteredHits), COUNT(selectedFromCleanedRecords)) as hitRatio, Ratio(COUNT(filteredMiss), COUNT(selectedFromCleanedRecords)) as errorRatio, GetYear(CurrentTime()) as year, GetMonth(CurrentTime()) as month, GetDay(CurrentTime()) as day, GetHour(CurrentTime()) as hour;
STORE selected INTO '$output/$DATE' USING PigStorage('\t');
