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
        artist VARCHAR(100),
        auth VARCHAR(50),
        firstName VARCHAR(50),
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR(50),
        length FLOAT,
        level VARCHAR(50),
        location TEXT,
        method VARCHAR(25),
        page VARCHAR(35),
        registration FLOAT,
        sessionId INT,
        song TEXT,
        status INT,
        ts INT,
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
        artist_name VARCHAR(100), 
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
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender CHAR(1),
        level INT
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id VARCHAR(30) PRIMARY KEY NOT NULL,
        title VARCHAR(100),
        artist_id VARCHAR(30),
        year INT,
        duration FLOAT)
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id VARCHAR(30) PRIMARY KEY NOT NULL,
        name VARCHAR(50),
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
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH']) ####################### there's something wrong with this copy function, possibly incompatible data types

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS {}
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT( se.ts as start_time,
            se.userId as user_id,
            se.level as level,
            ss.song_id as song_id,
            se.artist_id,
            se.sessionId as session_id,
            se.location as location,
            se.userAgent as user_agent)
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title
    WHERE se.page = 'NextPage'; 
""") # do I need the (;)?

user_table_insert = ("""
    INSERT INTO users(user_id,first_name,last_name,gender,level)
    SELECT( userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender as gender,
            level as level)
    FROM staging_events 
    GROUP BY userId;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id,title,artist_id,year,duration)
    SELECT( song_id,
            title,
            artist_id,
            year,
            duration)
    FROM staging_songs
    GROUP BY song_id;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id,name,location,latitude,longitude)
    SELECT( artist_id,
            name,
            location,
            latitude,
            longitude)
    FROM staging_events
    GROUP BY artist_id;
""")

time_table_insert = ("""
    INSERT INTO time(start_time,hour,day,week,month,year,weekday)
    SELECT 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
