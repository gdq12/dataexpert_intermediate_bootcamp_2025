----------------------------- saving files to csvs from within the runnin postgres container ----------------------------

-- from running container launch via ../setup_postgres/README.md
-- actor_films
COPY (SELECT * FROM actor_films)
TO '/tmp/actor_films.csv'
WITH CSV HEADER;
-- arena
COPY (SELECT * FROM arena)
TO '/tmp/arena.csv'
WITH CSV HEADER;
-- devices
COPY (SELECT * FROM devices)
TO '/tmp/devices.csv'
WITH CSV HEADER;
-- events
COPY (SELECT * FROM events)
TO '/tmp/events.csv'
WITH CSV HEADER;
-- game_details
COPY (SELECT * FROM game_details)
TO '/tmp/game_details.csv'
WITH CSV HEADER;
-- games
COPY (SELECT * FROM games)
TO '/tmp/games.csv'
WITH CSV HEADER;
-- player_seasons
COPY (SELECT * FROM player_seasons)
TO '/tmp/player_seasons.csv'
WITH CSV HEADER;
-- teams
COPY (SELECT * FROM teams)
TO '/tmp/teams.csv'
WITH CSV HEADER;
-- users
COPY (SELECT * FROM users)
TO '/tmp/users.csv'
WITH CSV HEADER;

---------------------------------- copy to local directory from running docker ---------------------------------------

docker cp containerID:tmp/. postgres_data/.

---------------------------------- load data files to snowflake ------------------------------------------------------

-- create schema and grant needed permissions for loading data 
create schema dataexpert.postgres_data;
grant usage on schema dataexpert.postgres_data to role data_loader;
grant create table on schema dataexpert.postgres_data to role data_loader;

-- used snowflake UI to load tables to dataexpert.postgres_data:
-- https://docs.snowflake.com/en/user-guide/data-load-web-ui

------------------------------- setting up airflow creds for dag runs -------------------------------------------------
create role airflow_role;

create user AIRFLOW_USER password = 'AIRFLOW_PWD';

grant role airflow_role to user airflow_user;

create warehouse AIRFLOW_WAREHOUSE
warehouse_size = 'X-SMALL'
;

grant usage on warehouse airflow_warehouse to role airflow_role;

create schema airflow_basic;

grant usage on database dataexpert to role airflow_role;

grant usage on schema dataexpert.airflow_basic to role airflow_role;

grant usage on schema dataexpert.postgres_data to role airflow_role;

grant create table on schema dataexpert.airflow_basic to role airflow_role;

grant select on all tables in schema dataexpert.airflow_basic to role airflow_role;

grant select on all tables in schema dataexpert.postgres_data to role airflow_role;

grant delete on all tables in schema dataexpert.airflow_basic to role airflow_role;

alter user airflow_user set default_warehouse='AIRFLOW_WAREHOUSE';