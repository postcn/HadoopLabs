REGISTER 'hdfs:///user/root/postcn_Lab4/UpperCase-0.0.1-SNAPSHOT.jar';
DEFINE Upper edu.rosehulman.postcn.Upper();
records = LOAD '$input' using PigStorage('\t') AS (line:chararray);
words = FOREACH records GENERATE FLATTEN(TOKENIZE(line)) as word;
filtered = FILTER words BY word MATCHES '\\w+';
groups = GROUP filtered BY word;
word_count = FOREACH groups GENERATE Upper(group) AS word, COUNT(filtered) AS count;
STORE word_count INTO '$output' USING PigStorage(',');
