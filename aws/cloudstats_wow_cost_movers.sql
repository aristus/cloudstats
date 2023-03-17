/*
A quick analysis of cost changes week over week. This groups every combination of
account, pricing bucket, operation, and whatever site-specific tags you add. then
it compares the average daily cost over the previous 7-day period with the averages
from 3, 6, 9, and 12 weeks back.

*/
create or replace view cloudstats.cloudstats_wow_cost_movers as

/*
Don't use current_date as the pivot. Use the latest partition that has data but
minus 3 days. This is an unfortunate thing with the AWS feed: cost data can
often roll in 1-3 days late, especially for large S3 buckets.
*/
with latest_known_good_date as (
    select
      date_add('day', -3, cast(max(date) as timestamp))
                                              as max_dataset_date
    from cloudstats.cloudstats
    where
      date >= date_format(date_add('day', -10, current_date), '%Y-%m-%d')
),

cost_per_day as (
  select
    date                                      as date,

    floor(date_diff('day', cast(date as timestamp), max_dataset_date) / 7)
                                              as weeks_back,
                                            
    max_dataset_date                          as max_dataset_date,
    product_code                              as product_code,
    operation                                 as operation,
    pricing_bucket                            as pricing_bucket,

    /**** ADD YOUR SITE-SPECIFIC DIMENSIONS HERE & remember to update the group by. *****/

    sum(cost)                                 as cost,
    sum(cost_mrr)                             as cost_mrr,
    sum(usage)                                as usage
  from
    cloudstats.cloudstats
    inner join latest_known_good_date on 1=1
  where
    -- 13 weeks back (7 * 13 = 91)
    date >= date_format(date_add('day', -91, max_dataset_date), '%Y-%m-%d')
  group by 1,2,3,4,5,6
),

cost_per_week as (
select
    -- technically not the "week of" but the latest date in the 7-day period.
    -- also, you can't just do max(date) because if you group by many dimensions,
    -- some of those compound groups will not have data on all days. Instead start
    -- with max_dataset_date and subtract weeks_back
    --max(date) as week_of,
    cast(date(date_add('week', cast(-1 * weeks_back as int), max_dataset_date)) as varchar)
                                              as week_of,

    cast(date(max_dataset_date) as varchar)   as current_week,
    weeks_back                                as weeks_back,
    product_code                              as product_code,
    operation                                 as operation,
    pricing_bucket                            as pricing_bucket,

    /**** ADD YOUR SITE-SPECIFIC DIMENSIONS HERE *****/

    count(distinct date)                      as num_days,
    sum(cost) / 7                             as avg_cost_daily,
    sum(usage) / 7                            as avg_usage_daily
  from cost_per_day
  group by 1,2,3,4,5,6
),

consolidated as (
  select
    current_week                              as current_week,
    product_code                              as product_code,
    operation                                 as operation,
    pricing_bucket                            as pricing_bucket,

    /**** ADD YOUR SITE-SPECIFIC DIMENSIONS HERE *****/

    /*
    SQL standard avg() and count() ignore null values. Handy for this kind of aggregation.
    However when avg() gets zero rows, it returns null. And any value divided by null is null.
    Thank you for subscribing to SQL Facts!
    */
    avg(if(weeks_back = 0, avg_cost_daily, 0))              as current_avg_cost,
    avg(if(weeks_back between 1 and  3, avg_cost_daily, 0)) as avg_last_3,
    avg(if(weeks_back between 1 and  6, avg_cost_daily, 0)) as avg_last_6,
    avg(if(weeks_back between 1 and  9, avg_cost_daily, 0)) as avg_last_9,
    avg(if(weeks_back between 1 and 12, avg_cost_daily, 0)) as avg_last_12

  from cost_per_week
  group by 1,2,3,4
),

/*
In the beginning, the SQL null-value was created. This has made a lot of people very
angry and been widely regarded as a bad move.
*/
consolidated_nonnull as (
  select
    current_week                              as current_week,
    product_code                              as product_code,
    operation                                 as operation,
    pricing_bucket                            as pricing_bucket,

    /**** ADD YOUR SITE-SPECIFIC DIMENSIONS HERE *****/

    -- min value is epsilon to avoid divide-by-zero
    if(current_avg_cost is null, 0.00001, current_avg_cost) as current_avg_cost,
    if(avg_last_3 is null,       0.00001, avg_last_3)       as avg_last_3,
    if(avg_last_6 is null,       0.00001, avg_last_6)       as avg_last_6,
    if(avg_last_9 is null,       0.00001, avg_last_9)       as avg_last_9,
    if(avg_last_12 is null,      0.00001, avg_last_12)      as avg_last_12

  from consolidated
)

select
  abs(current_avg_cost - avg_last_3) as delta_3_abs, -- for sorting by absolute change
  current_week                                as current_week,
  product_code                                as product_code,
  operation                                   as operation,
  pricing_bucket                              as pricing_bucket,

  /**** ADD YOUR SITE-SPECIFIC DIMENSIONS HERE *****/

  current_avg_cost                            as current_avg_cost,
  current_avg_cost * 30.4375                  as current_cost_mrr,

  current_avg_cost - avg_last_3               as delta_3,
  avg_last_3                                  as avg_last_3,
  (current_avg_cost - avg_last_3) / avg_last_3 as delta_3_percent,
  current_avg_cost - avg_last_6               as delta_6,
  avg_last_6                                  as avg_last_6,
  (current_avg_cost - avg_last_6) / avg_last_6 as delta_6_percent,
  current_avg_cost - avg_last_9               as delta_9,
  avg_last_9                                  as avg_last_9,
  (current_avg_cost - avg_last_9) / avg_last_9 as delta_9_percent,
  current_avg_cost - avg_last_12              as delta_12,
  avg_last_12                                 as avg_last_12,
  (current_avg_cost - avg_last_12) / avg_last_12 as delta_12_percent
from consolidated_nonnull
order by 1 desc
;
