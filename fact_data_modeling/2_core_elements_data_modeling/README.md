### Get started with the lab

1. Spin up the containers from set up [README](../../setup_postgres/README.md)

    ```bash
    ~/git_repos/dataexpert_intermediate_bootcamp_2025/1_dimensional_data_modeling/1_setup_postgres

    make up
    ```

2. Go to PGadmin UI: [http://localhost:5050](http://localhost:5050) 

3. Go to server from the setup session. If starting from scratch follow setup instructions in [README](../1_setup_postgres/README.md)

### Cumulative users active tbl

1. create the DDL 

    ```sql
    create table users_cumulated (
    user_id text,
    dates_active date[],
    date date,
    primary key (user_id, date)
    )
    ;
    ```

    + additional notes: 

        - dates_active: list of dates in the past where the user was active 

        - date: current date of the user, aka the recorded last user date activation recorded by the tbl

        - `user_id ` must be a string type because the values take up too much memory as an int    

2. populate tbl 

    ```sql
    insert into users_cumulated
    with yesterday as 
    (select * 
    from users_cumulated
    where date = date('2022-12-31')
    ),
    today as 
    (select 
        user_id::text user_id, 
        date(event_time::timestamp) dates_active
    from events 
    where date(event_time::timestamp) = date('2023-01-01')
    and user_id is not null 
    group by 1, 2
    )
    select 
        coalesce(t.user_id, y.user_id::text) user_id,
        case 
            when y.dates_active is null 
                then array[t.dates_active]
            when t.dates_active is null 
                then y.dates_active
            else array[t.dates_active]||y.dates_active
            end dates_active,
        coalesce(t.dates_active, y.date + interval '1 day') date
    from today t 
    full outer join yesterday y on t.user_id = y.user_id::text
    ;
    ```

    + additional notes: 

        - in the today CTE, can define active users based on the url. example, did they log in, did they visit certain pages etc 

        - null user_ids are a relic of dev env of the website server, best to filter them out early on

        - `date` column is meant to represent the latest date the tbl was populated for that user, which is why `y.date + interval '1 day'` syntax is used for the user_id has no activity from the today CTE

        - when populating the `dates_active` col array, want to try to add the latest date at the beginning of the array 

3. populate incrementally

    ```sql
    insert into users_cumulated
    with yesterday as 
    (select * 
    from users_cumulated
    where date = date('2023-01-12')
    ),
    today as 
    (select 
        user_id::text user_id, 
        date(event_time::timestamp) dates_active
    from events 
    where date(event_time::timestamp) = date('2023-01-13')
    and user_id is not null 
    group by 1, 2
    )
    select 
        coalesce(t.user_id, y.user_id::text) user_id,
        case 
            when y.dates_active is null 
                then array[t.dates_active]
            when t.dates_active is null 
                then y.dates_active
            else array[t.dates_active]||y.dates_active
            end dates_active,
        coalesce(t.dates_active, y.date + interval '1 day') date
    from today t 
    full outer join yesterday y on t.user_id = y.user_id::text
    ;
    ```

    + additional notes: 

        - just manualy updated the date in yesterday and today CTEs accordingly to popuate the tbl 

4. Implementing `datelist` with bit32 for dates_active

    ```sql
    with users as 
    (select *
    from users_cumulated 
    where date = date('2023-01-31')
    ),
    series as 
    (select * 
    from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') series_date
    ),
    place_holder_ints as 
    (select 
        case 
            -- is the series date within the engagement date list 
            when dates_active @> array[date(series_date)]
                -- the number of days between current date and series date to the power of 2
                then pow(2, 32 - (date - date(series_date)))::bigint
            else 0
            -- ::bit(32) --> turns the calc value into a bit value (takes up less memory)
            end placeholder_int_value,
        *
    from users 
    cross join series 
    -- where user_id = '1019427114861370600'
    )
    select 
        user_id,
        (sum(placeholder_int_value)::bigint)::bit(32) bit_map,
        -- checks to see if there was at one '1' within the bit map for the month 
        bit_count((sum(placeholder_int_value)::bigint)::bit(32)) > 0 dim_is_monthly_active,
        -- checks to see if there was at least one '1' within the last 7 positions in the bitmap
        bit_count('11111110000000000000000000000000'::bit(32) & 
                    (sum(placeholder_int_value)::bigint)::bit(32)) > 0 dim_is_weekly_active,
        -- checks to see if there was at least one '1' within the last position in the bitmap
        bit_count('10000000000000000000000000000000'::bit(32) & 
                    (sum(placeholder_int_value)::bigint)::bit(32)) > 0 dim_is_daily_active
    from place_holder_ints
    group by user_id
    order by 4 desc
    ;
    ```

    + additional notes: 

        - can decide on the bit size (aka the array size)

        - at fb, they kept it to 28 days (bit28) so would always have exactly 4 weeks per month and each week day was repeated exactly 4x. This elimiinates seasonality in the data 

        - `placeholder_int_value` converts all the active dates to 2^32 --> converts the activation of a given calendar month as a bit map of 1s and 0s --> when visually scan this, easily see this frequency within the bit map

        - `&` bit operator compares the bit map between 2 variables 

        - computers compute the fatest using bites