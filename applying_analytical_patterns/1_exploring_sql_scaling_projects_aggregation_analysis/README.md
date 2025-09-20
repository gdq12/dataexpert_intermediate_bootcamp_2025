### Get started with the lab

1. Spin up the containers from set up [README](../../setup_postgres/README.md)

    ```bash
    ~/git_repos/dataexpert_intermediate_bootcamp_2025/1_dimensional_data_modeling/1_setup_postgres

    make up
    ```

2. Go to PGadmin UI: [http://localhost:5050](http://localhost:5050) 

3. Go to server from the setup session. If starting from scratch follow setup instructions in [README](../1_setup_postgres/README.md)

### Growth accounting tbl

1. create table 

```sql
create table user_growth_accounting (
    user_id text,
    first_active_date date, 
    last_active_date date,
    daily_active_state text,
    weekly_active_state text,
    dates_active date[],
    date date,
    primary key (user_id, date)
)   
```

+ additional notes:

    - limitation with cummulation tbl is that there will be user activity that wont be logged from the very beginning of their use cycle because the building of the cummulation tble didnt start when they first appeared as a user. It is what is, this effect diminishes over time 

2. populating the tbl 

    ```sql 
    insert into user_growth_accounting
    with yesterday as 
    (select * 
    from user_growth_accounting 
    where date = '2023-01-09'
    ),
    today as 
    (select 
        user_id::text user_id, 
        date_trunc('day', event_time::timestamp) today_date,
        count(1)
    from events 
    where date_trunc('day', event_time::timestamp) = '2023-01-10'
    and user_id is not null 
    group by 1, 2
    )
    select 
        coalesce(t.user_id, y.user_id) user_id,
        coalesce(y.first_active_date, t.today_date) first_active_date,
        coalesce(t.today_date, y.last_active_date) last_active_date,
        case 
            when y.user_id is null and t.user_id is not null 
                then 'New'
            when y.last_active_date = t.today_date - interval '1 day'
                then 'Retained'
            when y.last_active_date < t.today_date - interval '1 day'
                then 'Resurrected'
            when t.today_date is null and y.last_active_date = y.date
                then 'Churned'
            else 'Stale'
        end daily_active_state,
        case 
            when y.user_id is null and t.user_id is not null 
                then 'New'
            when y.last_active_date < t.today_date - interval '7 day'
                then 'Resurrected'
            when t.today_date is null and y.last_active_date = y.date - interval '7 day'
                then 'Churned'
            when coalesce(t.today_date, y.last_active_date) + interval '7 day' >= y.date 
                then 'Retained'
            else 'Stale'
        end weekly_active_state,
        coalesce(y.dates_active, array[]::date[])||
        case 
            when t.user_id is not null
                then array[t.today_date]
            else array[]::date[]
        end date_list,
        coalesce(t.today_date, y.date + interval '1 day') date
    from today t
    full outer join yesterday y on t.user_id = y.user_id 
    ;
    ```

    + additional notes:

        - `first_active_date`: when the user_id records come from the yesterday CTE, its not a new user so take the first calculated firs_active_date. When its a new user_id, wont come from the yesterday CTE so will come from the today CTE and so take the `today_date` instead 

        - `last_active_date`: opposite logic. When the user_id isnt new, aka from yesterday CTE, then want to take the last activation date found in the today CTE. If the last activation was before yesterday then take the last_activation_date value from the yesterday CTE. If they are a new user, then the first_activation_date will be the same as the last_activation_date

        - `daily_active_state`:

            * when user_id is only present in the today CTE then they are a new user 

            * when the max date for the user_id is 1 day before the today_date, then the user was active for at least 2 consecutive days in relation to today, they are retained

            * when the user_id is present in the yesteray CTE but more than 1 day before today_date, it means they were iactive for a couple of days but then returned, so they are resurrected

            * when the user_id is not present in the today CTE and the yesterday CTE captures the latest state of their activation then they are churned 

            * stale is when user_id churned out and never returned 

            * based on these descriptions, a given user may do the following: active --> retained --> churned --> resurrected etc

        - `weekly_active_state`: it is the same thing as daily_active_state, except it looks for a change in state within the last 7 days of the `date` column

### Analysis queries that showcase the logic 

1. understanding the weekly retained logic

    ```sql
    select * 
    from user_growth_accounting
    where user_id = '12205325038818400000'
    order by date
    ;
    ```

    + additional notes: 

        * this user was active on only jan1 and jan9

        * `daily_active_state`: New on jan1, churned on jan2, and resurrected on jan9

        * `weekly_active_state`: was retained for jan2-jan8 cause the (last_active_date + 7 days) was greater than the date (date when pipleine run), then resurrected on jan9 cause it was present in the today CTE **AND** the last_active_date from the yesterday CTE (jan1) predates the (today_date - 7days), then classified retained on jan10 cause it was still present in the today CTE and the (date_date + 7 days) is greater after date (date when pipeline run). 
        
        * BTW for retained, so long as the date from the (today CTE or the last_active_date from the yestready CTE + 7 days) is greater than yesterday CTE date, then the user_id is classified as retained bc active within the last 7 days 

### Retention analysis for cohorts

* how many users are there in the cohort?

    ```sql
    select 
        date, 
        count(1) num_rows
    from user_growth_accounting
    where first_active_date = '2023-01-01'
    group by 1
    order by 1
    ;
    ```

    + additional info: here see that working with same number of users from day1 of calc info 

* calculate the trend of activation 

    ```sql
    select 
        date, 
        count(case 
                    when daily_active_state in ('New', 'Retained', 'Resurrected')
                        then 1 
                end) active_count, 
        count(1) num_rows,
        count(case 
                    when daily_active_state in ('New', 'Retained', 'Resurrected')
                        then 1 
                end)::real/count(1) active_perc
    from user_growth_accounting
    where first_active_date = '2023-01-01'
    group by 1
    order by 1, 2
    ;
    ```

    + additional notes:

        - can see that activation is quite high in the begining and then a sharp decline

        - the rate of decline seems to decrease around day 8

        - this depicts the J curve

        - this approach is good to take when looking at a single cohort but not good when looking at multiple cohorts 

* analyze retention based on number of days from first activation 

    ```sql
    select 
        date - first_active_date days_since_firt_active, 
        count(case 
                    when daily_active_state in ('New', 'Retained', 'Resurrected')
                        then 1 
                end) active_count, 
        count(1) num_rows,
        count(case 
                    when daily_active_state in ('New', 'Retained', 'Resurrected')
                        then 1 
                end)::real/count(1) active_perc
    from user_growth_accounting
    group by 1
    order by 1, 2
    ;
    ```

* can also look to see which day of the week has the most activation

    ```sql
    select
        extract(dow from first_active_date) dow_activation,
        date - first_active_date days_since_firt_active, 
        count(case 
                    when daily_active_state in ('New', 'Retained', 'Resurrected')
                        then 1 
                end) active_count, 
        count(1) num_rows,
        count(case 
                    when daily_active_state in ('New', 'Retained', 'Resurrected')
                        then 1 
                end)::real/count(1) active_perc
    from user_growth_accounting
    group by 1, 2
    order by 1, 2, 3
    ;
    ```