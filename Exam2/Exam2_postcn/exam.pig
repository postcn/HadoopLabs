REGISTER 'hdfs:///tmp/Exams/postcnUDF.jar';
DEFINE Concat edu.rosehulman.postcn.Concat();
DEFINE Convert edu.rosehulman.postcn.ScoreConvert();

grades = LOAD '$gradeInput' using PigStorage(',') AS (firstName:chararray, lastName:chararray, course:chararray, grade:double);
courses = LOAD '$courseInput' using PigStorage(',') AS (course: chararray, name:chararray);

joined = JOIN grades BY course, courses BY course;

cleaned = FOREACH joined GENERATE Concat($0, $1) AS name, $2 AS course, $5 AS courseName, Convert($3) AS letterGrade;
STORE cleaned INTO '$pigOutput/$username' USING PigStorage('\t');
