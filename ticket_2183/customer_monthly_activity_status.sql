{# Customer Monthly Activity Status #}
{{ config(
  materialized="incremental",
  unique_key=["customer_uid","year_month"],
  partition_by={"field": "year_month", "data_type": "date", "granularity": "day"},
  cluster_by=["customer_uid"]
) }}

with month_activity as (
  -- Tiger
  select
    customer_uid,
    date_trunc(date(ticket_delivery_date), month) as year_month
  from {{ goldrush_data_dbt_common_macros.xref('goldrush_data_dbt_sportsbook','fact_sportsbook') }}
  where ticket_delivery_date is not null
  {% if is_incremental() %}
    and date_trunc(date(ticket_delivery_date), month) > date_trunc(date_sub(current_date(), interval 1 month), month)
    and not exists (
    select 1
    from {{ this }} t
    where t.year_month = date_trunc(date(ticket_delivery_date), month)
      and t.customer_uid = customer_uid
  )
  {% endif %}

  union distinct
  -- Kambi
  select
    customer_uid,
    date_trunc(date(coalesce(ticket_delivery_date, kpi_date)), month) as year_month
  from {{ ref('src_kambi_360') }}
  where coalesce(ticket_delivery_date, kpi_date) is not null
  {% if is_incremental() %}
    and date_trunc(date(coalesce(ticket_delivery_date, kpi_date)), month)
        > date_trunc(date_sub(current_date(), interval 1 month), month) 
    and not exists (
    select 1
    from {{ this }} t
    where t.year_month = date_trunc(date(coalesce(ticket_delivery_date, kpi_date)), month)
      and t.customer_uid = customer_uid
  )
  {% endif %}

),
first_activity as (
  select customer_uid, min(year_month) as first_activity_month
  from month_activity 
  group by 1
),
prev_month_activity as (
  select customer_uid, year_month as prev_month from month_activity
)
select
  ma.customer_uid,
  ma.year_month,
  case
    when fa.first_activity_month = ma.year_month then 'Acquired'
    when pma.prev_month is not null      then 'Retained'
    when fa.first_activity_month < ma.year_month and pma.prev_month is null then 'Reactivated'
    else 'Undefined'
  end as activity_status
from month_activity ma
left join first_activity fa on fa.customer_uid = ma.customer_uid
left join prev_month_activity pma
  on pma.customer_uid = ma.customer_uid
 and pma.prev_month = date_sub(ma.year_month, interval 1 month)
