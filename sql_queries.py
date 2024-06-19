import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE log_data"
staging_songs_table_drop = "DROP TABLE song_data"
songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"



# CREATE TABLES
# Staging Tables:
staging_events_table_create= ("""
    CREATE TABLE log_data(
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
    userAgent VARCHAR(100)
    userid INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE song_data(
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
    songplay_id VARCHAR(20),
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
    user_id VARCHAR(20) PRIMARY KEY,
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
    COPY log_data 
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS JSON '{}'
    REGION '{}'
    TIMEFORMAT 'epochmillisecs';
""").format()

staging_songs_copy = ("""
""").format()


# 
staging_events_copy = ("""
COPY staging_events FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_songs FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

