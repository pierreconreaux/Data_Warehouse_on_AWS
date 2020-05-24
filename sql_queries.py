import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
  artist        varchar,
  auth          varchar,
  firstName     varchar,
  gender        varchar(1),
  iteminsession int,
  lastname      varchar,
  length        float,
  level         varchar,
  location      varchar,
  method        varchar,
  page          varchar,
  registration  float,
  sessionid     int,
  song          varchar,
  status        int,
  ts            bigint,
  useragent     varchar,
  userid        int
);
""")

# note that we call ts as a bigint to speedup the process
# but we'll convert it to timestamp in other tables 
# to give confort to the marketing team (they'll rather see something like 2018-11-27 18:09:11 as date than a number serie)

                              
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
  num_songs        int,
  artist_id        varchar,
  artist_latitude  float,
  artist_longitude float,
  artist_location  varchar,
  artist_name      varchar,
  song_id          varchar,
  title            varchar,
  duration         float,
  year             smallint
);
""")

# to choose sortkey and disckey we'll need more discussion with marketing team
# for the exercice we'll presume that recent data is queried most frequently
# so we'll specify the timestamp column as the leading column for the sort key.
# in other case we'll specify the join column as both the sort key and the distribution key. 
# https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
  songplay_id     bigint IDENTITY(0,1) PRIMARY KEY,  
  start_time      timestamp REFERENCES time(start_time) sortkey,
  user_id         int REFERENCES users(user_id),
  level           varchar,
  song_id         varchar REFERENCES songs(song_id),
  artist_id       varchar REFERENCES artists(artist_id),
  session_id      int,
  location        varchar,
  user_agent      varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
  user_id         int NOT NULL PRIMARY KEY sortkey distkey,  
  first_name      varchar,
  last_name       varchar,
  gender          varchar(1),  
  level           varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
  song_id         varchar NOT NULL PRIMARY KEY sortkey distkey,  
  title           varchar ,
  artist_id       varchar ,
  year            smallint,  
  duration        float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
  artist_id       varchar NOT NULL PRIMARY KEY sortkey distkey,  
  name            varchar ,
  location        varchar,
  latitude        float,  
  longitude       float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
  start_time      timestamp NOT NULL PRIMARY KEY sortkey,  
  hour            int ,
  day             int ,
  week            int ,  
  month           int ,
  year            int ,
  weekday         int    
);
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))


config.get("S3","LOG_DATA")

staging_songs_copy = ("""
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(
start_time, 
user_id, 
level, 
song_id, 
artist_id, 
session_id, 
location, 
user_agent)
SELECT (timestamp 'epoch' + events.ts/1000 * interval '1 second') as start_time
,events.userid as user_id
,events.level as level
,songs.song_id as song_id
,songs.artist_id as artist_id
,events.sessionid as session_id
,events.location as location
,events.useragent as user_agent
FROM staging_events events
JOIN staging_songs songs ON events.song = songs.title AND events.artist = songs.artist_name;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_events
where userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
where artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour,day,week,month,year,weekday)
    SELECT  DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
                EXTRACT(hour from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                EXTRACT(day from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                EXTRACT(week from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                EXTRACT(month from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                EXTRACT(year from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                EXTRACT(weekday from (timestamp 'epoch' + ts/1000 * interval '1 second'))
    FROM    staging_events
    WHERE staging_events.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]