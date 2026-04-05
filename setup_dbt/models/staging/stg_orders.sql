{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='delete+insert'
    )
}}

with
    staging as (
        select
            id
            , user_id
            , order_date
            , status
        from {{ source('bootcamp', 'js_raw_orders') }}
    )

select *
from staging

{% if is_incremental() %}

    where order_date > (select max(order_date) from {{ this }} )

{% endif %}
