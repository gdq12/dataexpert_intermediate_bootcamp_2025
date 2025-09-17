### Get started with the lab

1. Spin up the containers from set up [README](../../setup_postgres/README.md)

    ```bash
    ~/git_repos/dataexpert_intermediate_bootcamp_2025/1_dimensional_data_modeling/1_setup_postgres

    make up
    ```

2. Go to PGadmin UI: [http://localhost:5050](http://localhost:5050) 

3. Go to server from the setup session. If starting from scratch follow setup instructions in [README](../1_setup_postgres/README.md)

### Buiding metric tbl 

1. create table 

    ```sql 
    create table array_metrics (
        user_id text,
        month_start date,
        metric_name text,
        metric_array real[],
        primary key (user_id, month_start, metric_name)
    )
    ;
    ```

2. populate tbl with daily stats into array

    ```sql
    insert into array_metrics
    with daily_aggregate as 
    (select
        user_id::text user_id,
        date(event_time) date,
        count(1) num_site_hits
    from events
    where date(event_time) = '2023-01-01' -- increase this by 1 day per execution
    and user_id is not null
    group by 1, 2
    ),
    yesterday_aggregate as 
    (select * 
    from array_metrics
    where date(month_start) = '2023-01-01'
    )
    select 
        coalesce(da.user_id, ya.user_id) user_id,
        coalesce(ya.month_start, date_trunc('month', da.date)) month_start,
        'site_hits' metric_name,
        case 
            when ya.metric_array is not null 
                then ya.metric_array||array[coalesce(da.num_site_hits, 0)]
            else array_fill(0, array[coalesce(da.date-coalesce(ya.month_start, date(date_trunc('month', da.date))), 0)])
                ||array[coalesce(da.num_site_hits, 0)]
        end metric_array
    from daily_aggregate da 
    full outer join yesterday_aggregate ya on da.user_id = ya.user_id
    on conflict (user_id, month_start, metric_name)
    do
        update set metric_array = excluded.metric_array
    ;
    ```

    + additional notes:

        - this query first calc the daily stats per user, then nests the values into an array so in the end there is only 1 value per user_id and reporting month 

        - `metric_array`

            * the values are filled sequentually, where values from the first reporting day are always the first index and the consecutive days are filled in after that sequentially 

            * when a given user starts creating activity stats a day post the first day of the month, `array_fill(0, array[coalesce(da.date-coalesce(ya.month_start, date(date_trunc('month', da.date))), 0)])` is used to fill the indexes of those null days with zero

            * the goal of thos array is to make sure that it is the same lenght at the end of each run irrespective if the user started creating activity on the first day of the month or not

        - `month_start`

            * in the lesson is is defined as `coalesce(ya.month_start, date_trunc('month', da.date))` but think it should be `coalesce(date_trunc('month', da.date), ya.month_start) month_start` becuase the concept of the join is to first fetch records from the today CTE, id the record doesnt exist for that user_id then it should be extracted from the yesterday CTE

3. check to make sure each user_id has the same array length

    ```sql 
    select 
        cardinality(metric_array),
        count(1)
    from array_metrics
    group by 1
    ;
    ```

    + additional notes: `cardinality()` counts the num of values in the array

### Dimensional analysis on daily metrics

```sql
with agg as
(select 
    metric_name, 
    month_start,
    array[sum(metric_array[1]),
        sum(metric_array[2]),
        sum(metric_array[3]),
        sum(metric_array[4])
        ] summed_array
from array_metrics
group by 1,2
) 
select
    metric_name, 
    month_start + cast(cast(index - 1 as text)||'day' as interval) event_date,
    metric_sum
from agg 
cross join unnest(agg.summed_array) 
    with ordinality as a(metric_sum, index)
;
```

+ additional notes

    - this sums up all the value according to the index position without have to explode the arrays to calc the total metric value per day 

    - draw back is that have to manually add `sum(metric_array[#])` for every day of the month 

    - the benefit of this approach is that not needed to explode the arrays to go back to dily stats analysis --> its like have little dfs within each row `metric_array` and just doing a sum according to the "columnar" index