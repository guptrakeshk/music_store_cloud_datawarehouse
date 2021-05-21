import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE STAGING TABLES

# The staging_events table is used to stage all events log data from the data source.
#
# In the ETL process, the data is first extracted from the source file, in this case
# source data file is stored in S3 location.
# Using COPY command one can efficiently extract/copy from source data and load into
# the staging destination, in this case staging destination is staging_events table.
staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events
        (artist            VARCHAR ,
        auth               VARCHAR,
        firstName          VARCHAR,
        gender             VARCHAR,
        itemInSession      INTEGER ,
        lastName           VARCHAR,
        length             FLOAT ,
        level              VARCHAR,
        location           VARCHAR,
        method             VARCHAR,
        page               VARCHAR,
        registration       FLOAT,
        sessionId          INTEGER,
        song               VARCHAR,
        status             INTEGER,
        ts                 TIMESTAMP,
        userAgent          VARCHAR,
        userId             INTEGER
        )
""")

# The staging_songs table is used to stage all songs data from the data source.
#
# In the ETL process, the data is first extracted from the source file, in this case
# source data file is stored in S3 location.
# Using COPY command one can efficiently extract/copy from source and load into
# the staging destination, in this case staging destination is staging_songs table.

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs
        (artist_id         VARCHAR,
        artist_latitude    FLOAT,
        artist_location    VARCHAR,
        artist_longitude   FLOAT,
        artist_name        VARCHAR,
        duration           FLOAT,
        num_songs          INTEGER,
        song_id            VARCHAR,
        title              VARCHAR,
        year               INTEGER
        )
""")



# SQL queries to create database tables for fact and dimension tables
# CREATE TABLES

# 
songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays 
                         (songplay_id int identity(0,1) PRIMARY KEY, 
                         start_time timestamp, 
                         user_id int, 
                         song_id varchar, 
                         artist_id varchar, 
                         session_id int, 
                         location varchar, 
                         user_agent varchar) 
                         DISTKEY (song_id) 
                         COMPOUND SORTKEY (song_id,artist_id);
                         """)

user_table_create = (""" CREATE TABLE IF NOT EXISTS users 
                        (user_id int NOT NULL PRIMARY KEY, 
                        first_name varchar, 
                        last_name varchar, 
                        gender varchar, 
                        level varchar) 
                        diststyle all 
                        COMPOUND SORTKEY(first_name, last_name);
                        """)

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs 
                        (song_id varchar NOT NULL PRIMARY KEY, 
                        title varchar NOT NULL, 
                        artist_id varchar NOT NULL, 
                        year int, 
                        duration numeric) 
                        DISTKEY (song_id) 
                        SORTKEY (artist_id);
                        """)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists 
                        (artist_id varchar NOT NULL PRIMARY KEY, 
                        name varchar NOT NULL, 
                        location varchar,
                        latitude numeric, 
                        longitude numeric) 
                        diststyle all 
                        COMPOUND SORTKEY (name,location);
                        """)

time_table_create = (""" CREATE TABLE IF NOT EXISTS time 
                        (start_time timestamp NOT NULL PRIMARY KEY, 
                        hour int, 
                        day int, 
                        week varchar,
                        month int, 
                        year int, 
                        weekday varchar) 
                        DISTKEY (start_time);
                        """)


# Get configurations value from the configuration file 
LOG_DATA=config.get('S3','LOG_DATA')
SONG_DATA=config.get('S3','SONG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
IAM_ROLE_ARN=config.get('IAM_ROLE','ARN')
REGION='us-west-2'

#print("ARN = ", IAM_ROLE_ARN)

# STAGING TABLES 

# SQL query to copy events log data using COPY command to staging events table 
# Note: TIMEFORMAT option is recommended to avoid any data value that is in epoch time
# refer https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat
staging_events_copy = (""" COPY staging_events FROM {} 
                        iam_role {}  
                        JSON {}   
                        region  '{}'
                        TIMEFORMAT AS 'epochmillisecs' ;
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH, REGION)


# SQL query to copy songs data using COPY command to staging songs table
# Note: JSON 'auto' is recommended to avoid any data value that might contain delimiter 
# causing an error codes 1201 or 1214. 
# refer https://docs.aws.amazon.com/redshift/latest/dg/r_Load_Error_Reference.html

staging_songs_copy = (""" COPY staging_songs FROM {} 
                        iam_role {}      
                        region '{}'   
                        format as json 'auto';
""").format(SONG_DATA,IAM_ROLE_ARN, REGION)


# FINAL TABLES

# SQL queries to insert records into database tables from staging tables
# INSERT RECORDS

# Note: Pay attention to SELECT distinct userId records where userId is NOT NULL
# It is to avoid any situation where userId is null and still attempt to get inserted into
# a NOT NULL primary key column of users table.
user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level ) 
                    SELECT distinct(userId), firstName, lastName, gender, level
                    FROM staging_events
                    WHERE userId IS NOT NULL;
                    """)

song_table_insert = (""" INSERT INTO songs(song_id, title, artist_id, year, duration) 
                    SELECT song_id, title, artist_id, year, duration
                    FROM staging_songs;
                    """)
# Note: Pay attention to SELECT distinct artist_id records where artist_id is NOT NULL.
# It is to avoid any situation where artist_id is null and still attempt to get inserted into
# a NOT NULL primary key column of artists table.
artist_table_insert = (""" INSERT INTO artists(artist_id, name, location, latitude, longitude) 
                    SELECT distinct(artist_id), artist_name, artist_location,artist_latitude, artist_longitude
                    FROM staging_songs
                    WHERE artist_id is NOT NULL;
                        """)

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id,
                            location, user_agent) 
                    SELECT log.ts, log.userId, song.song_id, song.artist_id, log.sessionId,
                        log.location, log.userAgent
                    FROM staging_songs song
                    JOIN staging_events log ON (song.artist_name = log.artist);
                            """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                    SELECT  distinct(ts) as start_time,
                    EXTRACT (HOUR FROM ts) AS hour, 
                    EXTRACT (DAY FROM ts) as day,
                    EXTRACT (WEEK FROM ts) AS week,
                    EXTRACT (MONTH FROM ts) AS month,
                    EXTRACT (YEAR FROM ts) AS year,
                    EXTRACT (WEEKDAY FROM ts) AS weekday 
                    FROM staging_events;
                    """)

# QUERY LISTS

# List of queries to create staging and final tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

# List of queries to drop staging and final tables
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# List of queries to execute COPY command
copy_table_queries = [staging_events_copy, staging_songs_copy]

# List of queries to insert into final tables from staging tables.
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
