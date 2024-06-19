import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"



# CREATE TABLES
# Staging Tables:
staging_events_table_create= ("""
    CREATE TABLE staging_events(
    artist VARCHAR(50),
    auth VARCHAR(10),
    firstName VARCHAR(20),
    gender CHAR(1), 
    itemInSession INT,
    lastName VARCHAR(20),
    length Decimal,
    level VARCHAR(10),
    location VARCHAR(50),
    method VARCHAR(10),
    page VARCHAR(10),
    registration DECIMAL,
    sessionId INT,
    song VARCHAR(50),
    status INT,
    ts INT,
    userAgent VARCHAR(100),
    userid INT)
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
    num_songs INT,
    artist_id VARCHAR(20),
    artist_latitude DECIMAL,
    artist_longitude DECIMAL, 
    artist_location VARCHAR(50),
    artist_name VARCHAR(50), 
    song_id VARCHAR(20), 
    title VARCHAR(30), 
    duration DECIMAL, 
    year INT)
""")

# Final Tables:
songplay_table_create = ("""
    CREATE TABLE songplays(
    songplay_id BIGINT,
    start_time TIMESTAMP,
    user_id VARCHAR(20),
    level VARCHAR(10),
    song_id VARCHAR(20),
    artist_id VARCHAR(20),
    session_id INT,
    location VARCHAR(50),
    user_agent VARCHAR(20))
""")

user_table_create = ("""
    CREATE TABLE users(
    user_id VARCHAR(20) PRIMARY KEY NOT NULL,
    first_name VARCHAR(20),
    last_name VARCHAR(20),
    gender CHAR(1),
    level INT
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
    song_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(50),
    artist_id VARCHAR(20),
    year INT,
    duration DECIMAL)
""")

artist_table_create = ("""
    CREATE TABLE artists(
    artist_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(20),
    location VARCHAR(50),
    latitude DECIMAL,
    longitude DECIMAL)
""")

time_table_create = ("""
    CREATE TABLE time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT, 
    month INT,
    year INT,
    weekday CHAR(1)
    )
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) 
""")

user_table_insert = ("""
    INSERT INTO users(user_id,first_name,last_name,gender,level)
    VALUES(%s,%s,%s,%s,%s)
""")

song_table_insert = ("""
    INSERT INTO songs(song_id,title,artist_id,year,duration)
    VALUES(%s,%s,%s,%s,%s)
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id,name,location,latitude,longitude)
    VALUES(%s,%s,%s,%s,%s)
""")

time_table_insert = ("""
    INSERT INTO time(start_time,hour,day,week,month,year,weekday)
    VALUES(%s,%s,%s,%s,%s,%s,%s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]