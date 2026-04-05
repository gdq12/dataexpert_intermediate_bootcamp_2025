-- create needed sources 
create role dbt_role;

create role data_loader;

create user DBT_USER password = 'DBT_PWD';

grant role dbt_role to user DBT_USER;

create warehouse DBT_WAREHOUSE
warehouse_size = 'X-SMALL'
;

-- where dbt model data will be sourced/created/maintained
create database dataexpert;

create schema dataexpert.dbt_basic;

-- needed permission to load source data 
grant usage on database dataexpert to role data_loader;

grant usage on schema dataexpert.dbt_basic to role data_loader;

grant create table on schema dataexpert.dbt_basic to role data_loader;

-- needed permissions for project run
grant usage on database dataexpert to role dbt_role;

grant usage on schema dataexpert.dbt_basic to role dbt_role;

grant create table on schema dataexpert.dbt_basic to role dbt_role;

grant create view on schema dataexpert.dbt_basic to role dbt_role;

grant usage on warehouse dbt_warehouse to role dbt_role;

grant select on all tables in schema dataexpert.dbt_basic to role dbt_role;

-- for snowsight UI work
grant role data_loader to user gdq12;

grant role dbt_role to user gdq12;

-- -- eventually not needed

-- grant create stage on schema dataexpert.dbt_basic to role data_loader;
-- alter user dbt_user set default_warehouse='DBT_WAREHOUSE';
-- grant select on future tables in schema dataexpert.dbt_basic to role dbt_role;

-- -- stage for loading data 
-- CREATE STAGE my_s3_public_stage
--   URL = 's3://dbt-tutorial-public/long_term_dataset/'
--   FILE_FORMAT = (TYPE='CSV')
-- ;

-- -- get a list of all the files available in the public bucket 
-- LIST @my_s3_public_stage;

-- -- create tbls pre-data load
-- create transient table dataexpert.dbt_basic.js_raw_customer (
-- id text, 
-- name text
-- )
-- ;

-- copy into dataexpert.dbt_basic.js_raw_customer (
-- id, name
-- )
--     from @my_s3_public_stage/raw_customers.csv
--     FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"')
-- ;