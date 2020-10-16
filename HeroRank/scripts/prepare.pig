r1 = LOAD '$file' USING PigStorage('\t') AS (a:chararray, b:chararray);
r2 = GROUP r1 BY a;
r3 = foreach r2 generate group as a, r1 as b;
r4 = foreach r3 generate a, 1.0, FLATTEN(BagToTuple(b.b));
STORE r4 INTO '$out' USING PigStorage('\t');