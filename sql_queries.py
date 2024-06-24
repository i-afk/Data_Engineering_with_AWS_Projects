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
        artist TEXT,
        auth VARCHAR(50),
        firstName VARCHAR(50),
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR(50),
        length FLOAT,
        level VARCHAR(10),
        location TEXT,
        method VARCHAR(25),
        page VARCHAR(35),
        registration FLOAT,
        sessionId INT,
        song TEXT,
        status INT,
        ts BIGINT,
        userAgent TEXT,
        userId INT)
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs INT ,
        artist_id VARCHAR(50),
        artist_latitude FLOAT,
        artist_longitude FLOAT, 
        artist_location TEXT,
        artist_name TEXT, 
        song_id VARCHAR(20), 
        title TEXT, 
        duration FLOAT, 
        year INT)
""")

# Final Tables:
songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INT IDENTITY(0,1),
        start_time BIGINT,
        user_id VARCHAR(20),
        level VARCHAR(10),
        song_id VARCHAR(20),
        artist_id VARCHAR(20),
        session_id INT,
        location TEXT,
        user_agent VARCHAR(20))
""") 

user_table_create = ("""
    CREATE TABLE users(
        user_id VARCHAR(30) PRIMARY KEY NOT NULL,
        first_name TEXT,
        last_name TEXT,
        gender CHAR(1),
        level VARCHAR(10)
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id VARCHAR(30) PRIMARY KEY NOT NULL,
        title TEXT,
        artist_id VARCHAR(30),
        year INT,
        duration FLOAT)
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id VARCHAR(30) PRIMARY KEY NOT NULL,
        name TEXT,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT)
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP PRIMARY KEY NOT NULL, 
        hour INT,
        day INT,
        week INT, 
        month INT,
        year INT,
        weekday CHAR(1)
    )
""")  


# STAGING TABLES

staging_events_copy = """
COPY staging_events FROM {}
CREDENTIALS {}
REGION 'us-west-2'
FORMAT AS JSON {};
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS {}
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT  se.ts as start_time,
            se.userId as user_id,
            se.level as level,
            ss.song_id as song_id,
            ss.artist_id,
            se.sessionId as session_id,
            se.location as location,
            se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title
    WHERE se.page = 'NextPage'; 
""") 

user_table_insert = ("""
    INSERT INTO users(user_id,first_name,last_name,gender,level)
    SELECT DISTINCT
            userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender as gender,
            level as level
    FROM staging_events
    WHERE userID IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id,title,artist_id,year,duration)
    SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id,name,location,latitude,longitude)
    SELECT DISTINCT
            artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT
        DATEADD(SECOND, ts, '1970-01-01 00:00:00') AS start_time,
        DATEPART(HOUR, DATEADD(SECOND, ts, '1970-01-01 00:00:00')) AS hour,
        DATEPART(DAY, DATEADD(SECOND, ts, '1970-01-01 00:00:00')) AS day,
        DATEPART(WEEK, DATEADD(SECOND, ts, '1970-01-01 00:00:00')) AS week,
        DATEPART(MONTH, DATEADD(SECOND, ts, '1970-01-01 00:00:00')) AS month,
        DATEPART(YEAR, DATEADD(SECOND, ts, '1970-01-01 00:00:00')) AS year,
        CASE 
            WHEN DATEPART(WEEKDAY, DATEADD(SECOND, ts, '1970-01-01 00:00:00')) IN (1, 7) THEN 'N' 
            ELSE 'Y' 
        END AS weekday
    FROM staging_events
    WHERE ts IS NOT NULL;
""") 

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
