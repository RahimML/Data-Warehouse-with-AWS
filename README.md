# Introduction 
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
The role of a data engineer is to extract the data from S3. After that, staging them on Amazon Redshift. In order to help Sparkify's analytical team in finding useful insight the could help in making Sparkify a more advanced app,  transferring the data into dimensional tables is going to be the next step after staging the tables on RedShift. All the previous steps will be done by building an ETL pipeline that could handle the procedure.

# Database schema 
The schema of the data based will be a star schema. it will consist of a one fact table and four dimensional tables.
Fact table: songplay.
Dimesional tables: users, songs, artist and time.

# ETL pipeline 
The process of the ETL pipeline will start by loading the data of staging_events and Staging_songs tables from S3 using the load_staging_tables function and staging them on Amazon Redshift. After that, the ETL pipeline will insert all the needed columns that has been loaded from S3 in the fact and dimensional tables using the insert_tables function.

# Repository files 
1. Create_tables.py: This file should be run first and it will drop tables if exists and then creating all the needed table.
2. ETL.py: This file should be run secondly afte Create_tables.py and it is the ETL pipeline and it will load the data form S3 and staging them on Redshift and after that it will insert all needed columns in the fact and dimensional tables.
3. Sql_queries.py: This file contains all the drop, create, copy and insert statements where they will be used in Create_tables.py and ETL.py files.
4. Cfg.cfg : this file contains all the important Redshift and IAM role information that must be used in order to read the S3 files, create tables and finishing the ETL pipeline process. Information needed such as Host, database name, database user and its password and S3 file paths.

