import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
users_table_drop = "drop table if exists users"
songs_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= (""" CREATE table if not exists staging_events ( 
artist varchar,
auth varchar, 
firstname varchar,
gender varchar,
itemInSession integer,
lastname varchar,
length float, 
level varchar,
location varchar,
method varchar,
page varchar,
registration float, 
sessionId integer,
song varchar,
status integer,
ts timestamp, 
userAgent varchar,
UserId integer
)
""")

staging_songs_table_create = (""" CREATE table if not exists staging_songs(
num_songs integer,
artist_id varchar,
artist_latitude float,
artist_longitude float, 
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year integer
)
""")
songplay_table_create = (""" CREATE table if not exists songplay (
songplay_id integer IDENTITY(0,1) not null Primary key, 
start_time timestamp not null, 
user_id integer not null, 
level varchar, 
songs_id varchar , 
artist_id varchar , 
session_id integer, 
location varchar, 
user_agent varchar
) 
""")

users_table_create = (""" create table if not exists users(
user_id integer not null primary key , 
first_name varchar not null, 
last_name varchar not null, 
gender varchar, 
level varchar
)  
""")

songs_table_create = (""" create table if not exists songs (
songs_id varchar not null primary key,
title varchar, 
artist_id varchar, 
year integer,
duration float)
""")
artist_table_create = (""" create table if not exists artist (
artist_id varchar not null primary key,
name varchar,
location varchar,
latitude float, 
longitude float )
""")

time_table_create = (""" create table if not exists time (
start_time timestamp not null primary key,
hour integer not null,
day integer not null,
week integer not null,
month integer not null, 
year integer not null,
weekday integer not null
)
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
format as JSON {}
""").format(config.get("S3", "LOG_DATA"),
           config.get("IAM_ROLE", "ARN"),
           config.get("S3", "LOG_JSONPATH")
           )

staging_songs_copy = (""" copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON 'auto'
""").format(config.get("S3", "SONG_DATA"),
           config.get("IAM_ROLE", "ARN")
           )

# FINAL TABLES

songplay_table_insert = (""" insert into songplay(start_time ,user_id ,level ,songs_id ,artist_id ,session_id ,location ,user_agent )
select distinct to_timestamp(to_char(ev.ts, '9999-99-99 99:99:99') ,'YYYY-MM-DD HH24:MI:SS'),
ev.userid, ev.level, so.song_id, so.artist_id ,ev.sessionid , so.artist_location ,ev.userAgent
from staging_events ev
join staging_songs so 
on so.title=ev.song 
and so.duration=ev.length
and so.artist_name = ev.artist
""")

users_table_insert = (""" insert into users (user_id , first_name , last_name , gender, level )
select distinct userid, firstname, lastname, gender, level
from staging_events
where userid is not null
""")

songs_table_insert = (""" insert into songs(songs_id, title, artist_id, year, duration)
select distinct song_id, title, artist_Id, year, duration
from staging_songs
where song_id is not null
""")

artist_table_insert = (""" insert into artist(artist_id, name, location, latitude, longitude)
select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from staging_songs
where artist_id is not null
""")

time_table_insert = (""" insert into time (start_time, hour, day, week, month, year, weekday )
select distinct ts, extract(hr from ts), extract(day from ts), extract(w from ts), extract(mon from ts), extract(y from ts), extract(weekday from ts)
from staging_events
where ts is not null

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, users_table_create, songs_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, songs_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, users_table_insert, songs_table_insert, artist_table_insert, time_table_insert]
