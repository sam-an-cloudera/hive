set hive.stats.column.autogather=false;
-- Currently, a query with multiple FileSinkOperators are not supported.
set hive.mapred.mode=nonstrict;
CREATE TABLE T1_n19(key INT, val STRING);
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE T1_n19;
CREATE TABLE T2_n11(key INT, val STRING);
LOAD DATA LOCAL INPATH '../../data/files/kv2.txt' INTO TABLE T2_n11;
CREATE TABLE T3_n5(key INT, val STRING);
LOAD DATA LOCAL INPATH '../../data/files/kv3.txt' INTO TABLE T3_n5;
CREATE TABLE T4_n1(key INT, val STRING);
LOAD DATA LOCAL INPATH '../../data/files/kv5.txt' INTO TABLE T4_n1;

CREATE TABLE dest_co1(key INT, val STRING);
CREATE TABLE dest_co2(key INT, val STRING);
CREATE TABLE dest_co3(key INT, val STRING);

set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 3 MR jobs are needed.
-- When Correlation Optimizer is turned on, only a single MR job is needed.
EXPLAIN
INSERT OVERWRITE TABLE dest_co1
SELECT b.key, d.val
FROM
(SELECT x.key, x.val FROM T1_n19 x JOIN T2_n11 y ON (x.key = y.key)) b
JOIN
(SELECT m.key, n.val FROM T3_n5 m JOIN T4_n1 n ON (m.key = n.key)) d
ON b.key = d.key;

INSERT OVERWRITE TABLE dest_co1
SELECT b.key, d.val
FROM
(SELECT x.key, x.val FROM T1_n19 x JOIN T2_n11 y ON (x.key = y.key)) b
JOIN
(SELECT m.key, n.val FROM T3_n5 m JOIN T4_n1 n ON (m.key = n.key)) d
ON b.key = d.key;

set hive.optimize.correlation=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_co2
SELECT b.key, d.val
FROM
(SELECT x.key, x.val FROM T1_n19 x JOIN T2_n11 y ON (x.key = y.key)) b
JOIN
(SELECT m.key, n.val FROM T3_n5 m JOIN T4_n1 n ON (m.key = n.key)) d
ON b.key = d.key;

INSERT OVERWRITE TABLE dest_co2
SELECT b.key, d.val
FROM
(SELECT x.key, x.val FROM T1_n19 x JOIN T2_n11 y ON (x.key = y.key)) b
JOIN
(SELECT m.key, n.val FROM T3_n5 m JOIN T4_n1 n ON (m.key = n.key)) d
ON b.key = d.key;

set hive.optimize.correlation=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=10000000000;
-- Enable hive.auto.convert.join.
EXPLAIN
INSERT OVERWRITE TABLE dest_co3
SELECT b.key, d.val
FROM
(SELECT x.key, x.val FROM T1_n19 x JOIN T2_n11 y ON (x.key = y.key)) b
JOIN
(SELECT m.key, n.val FROM T3_n5 m JOIN T4_n1 n ON (m.key = n.key)) d
ON b.key = d.key;

INSERT OVERWRITE TABLE dest_co3
SELECT b.key, d.val
FROM
(SELECT x.key, x.val FROM T1_n19 x JOIN T2_n11 y ON (x.key = y.key)) b
JOIN
(SELECT m.key, n.val FROM T3_n5 m JOIN T4_n1 n ON (m.key = n.key)) d
ON b.key = d.key;

-- dest_co1, dest_co2 and dest_co3 should be same
-- SELECT * FROM dest_co1 x ORDER BY x.key, x.val;
-- SELECT * FROM dest_co2 x ORDER BY x.key, x.val;
SELECT SUM(HASH(key)), SUM(HASH(val)) FROM dest_co1;
SELECT SUM(HASH(key)), SUM(HASH(val)) FROM dest_co2;
SELECT SUM(HASH(key)), SUM(HASH(val)) FROM dest_co3;
