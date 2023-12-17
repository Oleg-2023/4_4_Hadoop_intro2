--
SET hive.enforce.bucketing=true; 
--***--
CREATE EXTERNAL TABLE IF NOT EXISTS people_tmp(
  idx int,
  user_id int, 
  firstname String, 
  lastname String, 
  sex String,
  email String,
  phone String,
  birthdate Date,
  job_title String,
  group_no Smallint) 
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  STORED AS TEXTFILE
  LOCATION '/user/user_one/data/people'
  TBLPROPERTIES ('skip.header.line.count' = '1');
--
 CREATE EXTERNAL TABLE IF NOT EXISTS people(
  idx int,
  user_id int, 
  firstname String, 
  lastname String, 
  sex String,
  email String,
  phone String,
  birthdate Date,
  job_title String,
  group_no Smallint) 
  CLUSTERED BY(group_no) INTO 10 BUCKETS
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  STORED AS TEXTFILE;
--  
FROM people_tmp
INSERT OVERWRITE TABLE people 
SELECT *;
--***--
CREATE EXTERNAL TABLE IF NOT EXISTS organizations_tmp(
  idx int,
  organ_id int,
  organ_name String,
  website String,
  country String,
  descr String,
  founded Smallint,
  industry String,
  empl_cnt int,
  group_no Smallint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/user_one/data/organizations'
TBLPROPERTIES ('skip.header.line.count' = '1');
--
CREATE TABLE IF NOT EXISTS organizations(
  idx int,
  organ_id int,
  organ_name String,
  website String,
  country String,
  descr String,
  founded Smallint,
  industry String,
  empl_cnt int,
  group_no Smallint
)
CLUSTERED BY(group_no) INTO 10 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
STORED AS TEXTFILE;
--
FROM organizations_tmp
INSERT OVERWRITE TABLE organizations 
SELECT *;
--***--
CREATE TABLE IF NOT EXISTS customers_tmp(
  idx int,
  custom_id int,
  firstname String,
  lastname String,
  company String,
  city String,
  country String,
  phone1 String,
  phone2 String,
  email String,
  subscript_date Date,
  website String,
  group_no Smallint,
  subscript_year Smallint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
STORED AS TEXTFILE
LOCATION '/user/user_one/data/customs'
TBLPROPERTIES ('skip.header.line.count' = '1');
--
CREATE TABLE IF NOT EXISTS customers(
  idx int,
  custom_id int,
  firstname String,
  lastname String,
  company String,
  city String,
  country String,
  phone1 String,
  phone2 String,
  email String,
  subscript_date Date,
  website String,
  group_no Smallint
)
PARTITIONED BY(subscript_year Smallint)
CLUSTERED BY(group_no) INTO 10 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
STORED AS TEXTFILE;
--
FROM customers_tmp
INSERT OVERWRITE TABLE customers
PARTITION(subscript_year = 2020)
SELECT idx, custom_id, firstname, lastname, company, city, country, 
       phone1, phone2, email, subscript_date, website, group_no 
WHERE subscript_year = 2020;
--
FROM customers_tmp
INSERT OVERWRITE TABLE customers
PARTITION(subscript_year = 2021)
SELECT idx, custom_id, firstname, lastname, company, city, country, 
       phone1, phone2, email, subscript_date, website, group_no 
WHERE subscript_year = 2021;
--
FROM customers_tmp
INSERT OVERWRITE TABLE customers
PARTITION(subscript_year = 2022)
SELECT idx, custom_id, firstname, lastname, company, city, country, 
       phone1, phone2, email, subscript_date, website, group_no 
WHERE subscript_year = 2022;