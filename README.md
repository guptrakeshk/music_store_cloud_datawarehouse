## Data Warehouse using AWS S3 and Redshift  


### A hands-on, project-based instruction for data warehousing ETL using AWS S3 and AWS Redshift

This is a collection of resources for data engineering ETL for a fictious a music company. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON event logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Email: gupt.rakeshk@gmail.com

This project walks through end-to-end data engineering steps that are needed in a typical project that uses AWS resources.
User is expected to extracts data from AWS S3, stage them in Redshift, and transforms data into a set of dimensional tables and fact table. This ETL program is expected to create a datawarehouse in AWS cloud for their analytics team to continue finding insights in what songs their users are listening to more efficiently and quickly.

Note: The data modeling is inspired by **STAR Schema** design guidelines to make it convenient for analysts to get insights easily without writing complex queries involving multiple joins.

#### Pre-Requisites
- Ensure AWS IAM user is created with read access to S3 as well as necessary access to AWS Redshift
- AWS Redshift is running with a cluster that contains the distributed database to host datawarehouse tables
- AWS IAM user has right trust policy set-up to access and perform ETL activities on AWS Redshift.
- Ensure IAM role ARN is tested for right permission to access S3 and Redshift resources.
- For security of confidential information, credentials details must be stored in safe configuration file.
- For better network efficiency, ensure Redshift cluster is created in the same region where S3 bucket holding events log and songs meta data is located .

### Steps involved are :
- Creating data models that is needed to stage source data first and subsequently loading after transformation. AWS Redshit is being used as database store.
- Copy data that are stored in AWS S3. 
- Apply transformation on staged data and load into dataware house tables using AWS Redshift.
- Once data is cleansed, transformed and loaded into final table following STAR Schema models, it is ready to asnwer queries for analytics.

### Data Modeling and Design steps :
- Model your Redshift datawarehouse schema for staging and final tables
- Design tables to answer the queries outlined in the project.
- Write Redshift DB Connection and Cursor statements
- Develop your CREATE statement for each of the tables that need to store Factual and Dimension data
- Load the data with INSERT statement for each of the tables using staging tables.
- Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist.
- Test by running the proper select statements with the correct WHERE clause

### Building ETL Pipeline steps:
- Execute `COPY` command for each event_data and songs_data source files from AWS S3 location.
- Ensure `COPY` commands are executed successfully and source data from event logs and songs log are staged correctly.
- Once data is staged in Redshift, execute `INSERT` statement that fetches data from staging tables into final tables.
- Once data is cleansed, transformed and loaded, it is ready to answer specific queries for analytics.
- Test by running `SELECT` statements after running the queries on your Redshift database tables.

Here are helpful steps in executing python programs in right sequence. You must execute **create_table.py** first 
in order to create database tables which are needed for storing fact and dimension data.
1. execute `python create_table.py` from CLI or other interface 
2. execute `python etl.py` from CLI or other interface 


#### `COPY` command tips
- Note: TIMEFORMAT option is recommended to avoid any data value that is in epoch time. 
-- refer https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat

- Note: JSON 'auto' is recommended to avoid any data value that might contain default delimiter "," causing an error codes 1201 or 1214. 
-- refer https://docs.aws.amazon.com/redshift/latest/dg/r_Load_Error_Reference.html


### Project Dataset 

#### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

Song data location : s3://udacity-dend/song_data

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"artist_id":"ARJNIUY12298900C91","artist_latitude":null,"artist_location":"","artist_longitude":null,"artist_name":"Adelitas Way","duration":213.9424,"num_songs":1,"song_id":"SOBLFFE12AF72AA5BA","title":"Scream","year":2009}
```

#### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

Log data location : s3://udacity-dend/log_data
Log data json path location: s3://udacity-dend/log_json_path.json

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what a event log looks like.
```
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}
```

JSONPath that is used in the `COPY` command for listing the fieldnames
reference : https://udacity-dend.s3.us-west-2.amazonaws.com/log_json_path.json

```
{
    "jsonpaths": [
        "$['artist']",
        "$['auth']",
        "$['firstName']",
        "$['gender']",
        "$['itemInSession']",
        "$['lastName']",
        "$['length']",
        "$['level']",
        "$['location']",
        "$['method']",
        "$['page']",
        "$['registration']",
        "$['sessionId']",
        "$['song']",
        "$['status']",
        "$['ts']",
        "$['userAgent']",
        "$['userId']"
    ]
}
```

### Project artifacts and programs

- `create_tables.py` : A python script to generate staging and final tables in AWS Redshift.
- `sql_queries.py` : A python script to store collection of DDL and DML SQL statements.
- `etl.py` : A python script to build an ETL pipeline that involves extracting source data from S3, `COPY` into intermediate staging tables and load into final tables after performing transformation. 

