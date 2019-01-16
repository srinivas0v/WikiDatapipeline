
DROP DATABASE IF EXISTS wiki CASCADE;
CREATE SCHEMA IF NOT EXISTS wiki;

DROP TABLE IF EXISTS wiki.wiki_stg;
CREATE EXTERNAL TABLE IF NOT EXISTS wiki.wiki_stg ( langcode varchar(20), category varchar(20),
page_title varchar(500), non_unique_views int, bytes_transferred int,
ymd varchar(10), hour varchar(10))
STORED AS ORC;


DROP TABLE IF EXISTS wiki.wiki_ods_join;
CREATE EXTERNAL TABLE IF NOT EXISTS wiki.wiki_ods_join ( page varchar(500), views int,
byte int, language varchar(200), project varchar(30), 
year varchar(10), month varchar(10), day varchar(10), hour varchar(10) )
STORED AS ORC;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;
DROP TABLE IF EXISTS wiki.wiki_final;
CREATE external TABLE wiki.wiki_final( 
page_title varchar(500), views int, bytes int, language varchar(250),
project varchar(200))
PARTITIONED BY (year VARCHAR(10), month VARCHAR(10),day varchar(10), hour varchar(10))
STORED AS ORC;