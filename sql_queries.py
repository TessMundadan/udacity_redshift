import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')


ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events \
                                 (\
                                 artist_name VARCHAR ,\
                                 auth VARCHAR ,\
                                 first_name VARCHAR ,\
                                 gender VARCHAR ,\
                                 item_in_session INT ,\
                                 last_name VARCHAR ,\
                                 duration DECIMAL ,\
                                 level VARCHAR ,\
                                 location VARCHAR ,\
                                 method VARCHAR ,\
                                 page VARCHAR ,\
                                 registration DECIMAL ,\
                                 session_id INT ,\
                                 title VARCHAR ,\
                                 status INT ,\
                                 ts BIGINT ,\
                                 user_agent VARCHAR ,\
                                 user_id INT\
                                 )\
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs \
                                 (\
                                 artist_id VARCHAR ,\
                                 artist_latitude DECIMAL ,\
                                 artist_location VARCHAR ,\
                                 artist_longitude DECIMAL ,\
                                 artist_name VARCHAR ,\
                                 duration DECIMAL ,\
                                 num_songs INT ,\
                                 song_id VARCHAR ,\
                                 title VARCHAR ,\
                                 year INT\
                                 )\
""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
                            (songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY sortkey,\
                             start_time BIGINT NOT NULL,\
                             user_id INT NOT NULL distkey,\
                             level VARCHAR, \
                             song_id VARCHAR NOT NULL , \
                             artist_id VARCHAR NOT NULL, \
                             session_id INT, \
                             location VARCHAR, \
                             user_agent VARCHAR)\
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users( user_id INT PRIMARY KEY sortkey distkey, \
                        first_name VARCHAR, \
                        last_name VARCHAR, \
                        gender VARCHAR, \
                        level VARCHAR) ;\
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR PRIMARY KEY sortkey, \
                         title VARCHAR, \
                         artist_id VARCHAR NOT NULL, \
                         year INT, \
                         duration DECIMAL) diststyle all;\
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR PRIMARY KEY sortkey, \
                          name VARCHAR, \
                          location VARCHAR, \
                          latitude DECIMAL, \
                          longitude DECIMAL) diststyle all; \
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time BIGINT PRIMARY KEY sortkey, \
                         hour INT, \
                         day INT, \
                         week INT, \
                         month INT, \
                         year INT, \
                         weekday INT) diststyle all;\
""")

# STAGING TABLES

staging_events_copy = """copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json {}
""".format(LOG_DATA,ARN,LOG_JSONPATH)


staging_songs_copy = """copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto'
""".format(SONG_DATA,ARN)


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time ,\
                             user_id  ,\
                             level , \
                             song_id  , \
                             artist_id  , \
                             session_id , \
                             location , \
                             user_agent)
                             SELECT DISTINCT ts ,\
                             user_id ,\
                             level , \
                             song_id  , \
                             artist_id  , \
                             session_id , \
                             location , \
                             user_agent \
                             FROM staging_events \
                             JOIN staging_songs \
                             ON  staging_events.title = staging_songs.title \
                             AND staging_events.artist_name = staging_songs.artist_name \
                             AND staging_events.duration = staging_songs.duration \
                             where page = 'NextSong'\
                             
""")

user_table_insert = ("""INSERT INTO users SELECT  DISTINCT user_id  , \
                        first_name , \
                        last_name , \
                        gender , \
                        level \
                        FROM staging_events 
                        where page = 'NextSong'\
""")

song_table_insert = ("""INSERT INTO songs SELECT DISTINCT song_id,\
                         title, \
                         artist_id , \
                         year , \
                         duration \
                         FROM staging_songs\
                         
""")

artist_table_insert = ("""INSERT INTO artists SELECT DISTINCT artist_id,\
                          artist_name , \
                          artist_location , \
                          artist_latitude , \
                          artist_longitude \
                          FROM staging_songs\
                         
""")

time_table_insert = ("""INSERT INTO time SELECT  distinct ts, \
                        EXTRACT(hour from new_ts), \
                        EXTRACT(day from new_ts), \
                        EXTRACT(week from new_ts), \
                        EXTRACT(month from new_ts), \
                        EXTRACT(year from new_ts), \
                        EXTRACT(weekday from new_ts) \
                        FROM 
                        (SELECT \
                         ts, \
                         '1970-01-01'::date + ts/1000 * interval '1 second' as new_ts \
                         FROM staging_events
                         where page = 'NextSong')\
""")

        
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
