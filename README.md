# README: 

## PERM Analysis (by Yue Tan)
Direcotry: ana_code
A zip file containing source code to clean data for the analytic Clean.class, Clean.jar, CleanMapper.class
Actual code can be found in etl_code directory


Direcotry: data_ingest
Code and commands for data ingestion

data_ingest/Upload.txt
//command to upload the source code and data to HDFS 

data_ingest/Download_result.txt
//command to get the cleaned data from HDFS to local



Direcotry: etl_code
Code for cleaning data
Command to run the code can be found in data_ingest/Upload.txt
INPUT DATA can be found in root directory: perm_input.csv
OUTPUT DATA: perm_output.csv


Direcotry: profiling_code
Data documentation with column information and data type


Direcotry: screenshots
Queries are run on Hive.
Input data is the output of MapReduce cleaned data.



## GNI Analysis (by Shuyang Ma)
Our groups will all work on perm vis sonship data but with different focus. We have included three separate readme files each describing our respective part of the analytic (We think this will be more clear to follow than mixing our analytics together).

The purpose of my part of the analytics is to discover the relationship between economic index GNI (Gross national income) and per visa sponsorships.

I will work on cleaning the GNI dataset, and merging it with the cleaned PERM dataset (provided by Tanyue) using Hive. The analytics will be conducted on the merged dataset.

The majority of analytics is done in hive. Some data cleaning is performed before importing to Hive using map reduce. This follows the normal cleaning step in Hw 7, but the java code has been updated since hw7.

To run the analytics, follow the step-by-step solution provided below. You could also choose to skip the map-reduce cleaning step and directly use the cleaned dataset provided in the shared project folder under dataset directory: project/dataset/clean_gni &  project/dataset/clean_perm, load those data into hive tables and run the queries as shown in the query script within the analytic directory (All queries are listed in sequence and step is clearly marked). Coping and pasting each query in the same order as it appears in the query script should be sufficient for running the analytics in hive.


Detailed steps:

1.  Move the cleaned PERM dataset (cleaned and provided by Tanyue) to peel using scp.

scp /Users/mashuyang/Desktop/part-r-00000.csv sm8068@peel.hpc.nyu.edu:/home/sm8068

2. Move the raw original GNI dataset to peel using scp.

scp /Users/mashuyang/Desktop/GNI_dataset.csv sm8068@peel.hpc.nyu.edu:/home/sm8068

3. Move above two datasets from Peel to hdfs

hdfs dfs -put part-r-00000.csv project/dataset/perm_clean.csv
hdfs dfs -put GNI_dataset.csv project/dataset/GNI_dataset.csv

4. Go to the directory that contains clean.jar, and run it with GNI_dataset.csv as input, and project/clean_gni_output as output directory (delete the directory first if it already exists)

hadoop jar clean.jar Clean project/dataset/GNI_dataset.csv /user/sm8068/project/clean_gni_output

5. Move the output of map reduce job to project/dataset and rename it to gni_clean.csv

hdfs dfs -mv project/clean_gni_output/part-m-00000 project/dataset/gni_clean.csv

6. Now the cleaning step is finished. Login to hive, and the rest of analytics are all conducted using hive queries. The hive query script (with the name shuyang) in the analytic directory contains all the queries needed to run my part of the
 analytic from the very start to the very end. The queries are also arranged in the correct order. The queries are documented with comments and should be easy to follow. 

Note: you could skip step 1-5 and directly enters step 6 if you directly use the gni_clean.csv provided in project/dataset directory on my hdfs.

7. The results of the HiveQL analytics will be stored as hive tables in hive database

8. If somehow you don't have access to my project/dataset folder (this is unlikely since I definitely set the ACL, but just in case this happens), please contact me and let me know.
 





## Wage Analysis (by Zhenyuan Liang):
Datasets:
My own dataset is about wage which is an auxiliary dataset
The main dataset of our team is about perm

wage_industries.csv is the raw dataset
wage_clean is the final clean dataset
perm_status is also the clean dataset from my teammate Yue Tan but includes the status of the perm application: certified, denied, withdrawn. I used this dataset for analyzing the certified applicants. This dataset is more important that perm.csv


Important Columns we used:
Wage:
Occ_title: occupation title
Tot_emp: total employment
a_mean: mean of the annually wage for all US workers in each occupation
a_median: median of the annually wage for all US workers in each occupation
h_mean: mean of the hourly wage for all US workers in each occupation
h_median: median of the hourly wage for all US workers in each occupation

Perm:
Status: whether the perm application is certified, denied, or withdrawn
Title: occupation title
Wage: annually wage for each applicant


Data Location in HDFS:
wage_industries: 
476_big_data/hw7/clean/input/wage_industries.csv

wage_clean:
476_big_data/hw9/input_wage/wage_clean.csv

perm_status.csv:
476_big_data/hw9/input_perm_status/perm_status.csv



Data Ingestion: (the code is in the data ingest folders, I have clearly labeled them)
I uploaded wage_industries.csv, wage_clean, perm.csv, perm_status.csv from my computer to hdfs.
I used scp to transfer each of the dataset from my computer to peel

Then, I used hdfs dfs -put to transfer each of the dataset from peel to hdfs



Data Cleaning: (the code is in the cleaning folders. Should be straightforward)
I cleaned the data twice, so I have two separate folders for cleaning code. 
Firstly, by mapreduce, I extracted the possible useful columns which are occ_title, tot_emp, h_mean, a_mean, h_median, and a_median.

For the extra 0 columns, you can drop them in excel. Or, you can use the same format of the second cleaning code which only output key and doesn't have reducer. Or, You can ignore it, since we can get rid of it after the grouping command in hive. 

Secondly, by mapreduce, I got rid of the rows containing missing values. The code only output key and doesn't have reducer. 



Data Profiling:
I uploaded the wage dataset into the hive and created a hive table for it. 

I showed the first 10 row of the dataset to inspect the data, so I can know what the dataset looks like. 
Select * from wage limit 10;

I counted the number of the rows by the hive command
select count(occ_title) from wage;

I calculated the average for each numeric columns by the hive command. 
select avg(tot_emp) from wage;
select avg(h_mean) from wage;
select avg(a_mean) from wage;
select avg(h_median) from wage;
select avg(a_median) from wage;




Analysis:
Create hive table for wage and perm status:
(Make sure that you have put the data in the directory. We have this steps in Data Ingest)

CREATE EXTERNAL TABLE wage(occ_title STRING, tot_emp BIGINT, h_mean DOUBLE, a_mean BIGINT, h_median DOUBLE, a_median BIGINT) COMMENT 'Bigdata-wage' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zl2878/476_big_data/hw9/input_wage';

CREATE EXTERNAL TABLE perm_status(status STRING, title STRING, skill STRING, wage BIGINT, citizenship STRING, education STRING) COMMENT 'Bigdata-perm' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zl2878/476_big_data/hw9/input_perm_status';


Make a directory for grouping wage (I grouped wage because I can get rid the duplicated rows, and also only extracted the most important columns):
(Note that I accidentally type wage_group as hive_group, and I used this directory for the rest of the code)
(Please do not group perm_status because this will make the analysis wrong)
hdfs dfs -mkdir 476_big_data/final_code/hive_group


Logged to beeline and grouped my wage_clean.csv by occ_title by the hive command. I saved the query result to the hdfs:
insert overwrite directory '/user/zl2878/476_big_data/final_code/hive_group' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT occ_title, avg(a_mean) FROM wage group by occ_title;



I created hive tables for both wage_group, and perm_status:
CREATE EXTERNAL TABLE wage_group(occ_title STRING, a_mean BIGINT) COMMENT 'Bigdata-wage' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zl2878/476_big_data/final_code/hive_group';

Make a directory for merged data:
hdfs dfs -mkdir 476_big_data/final_code/merge2


I merged wage_group.csv, and perm_status.csv by occ_title by hive command. I also extracted the "certified" rows only. I saved the query result to the hdfs
insert overwrite directory '/user/zl2878/476_big_data/final_code/merge2' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select occ_title, a_mean, wage from wage_group inner join perm_status on occ_title =title where status="Certified";

I created a hive table for wage_perm_certified:

CREATE EXTERNAL TABLE wage_perm_certified(occ_title STRING, a_mean BIGINT, wage BIGINT) COMMENT 'Bigdata-merge_certified' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zl2878/476_big_data/final_code/merge2';

I renamed the column a_mean to a_mean_national, and then I renamed the column wage to a_mean_perm:
ALTER TABLE wage_perm_certified CHANGE a_mean a_mean_national BIGINT;
ALTER TABLE wage_perm_certified CHANGE wage a_mean_perm_certified BIGINT;

I counted the number of rows in wage_perm_certified hive table where a_mean_national is larger than a_mean_perm:
select count(occ_title) from wage_perm_certified where a_mean_national > a_mean_perm_certified;

Output is 4740.

Eighth, I counted the number of rows in wage_perm_certified hive table where a_mean_national is smaller than a_mean_perm:
select count(occ_title) from wage_perm_certified where a_mean_national < a_mean_perm_certified;

Output is 3095.

3095 and 4740 are my results.

