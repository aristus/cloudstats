/*
Clear out data from N days back, then re-load from _01. This table is meant
to only contain billed usage rows and fully-discounted, amortized costs.
See the cloudstats_bill* files for a table that is used to recreate the formal
bill sent to accounting.

We go 65 days back because the root table, the AWS CUR, is a running account
and not an immutable log. During normal operation rows can be added or modified
weeks back as various accounting things happen. This is also why the _00 and _01
stages of the pipeline are implemented as views and not materialized tables.

todo: this does not work in Athena, which has no delete and no drop partition. :(
*/

delete from cloudstats.cloudstats
--where date >= date_format(date_add('day', -65, to_timestamp('{{ get_batch_id(ts) }}'), '%Y-%m-%d')
where date >= date_format(date_add('day', -65, current_date), '%Y-%m-%d')
;

insert into cloudstats.cloudstats
select * from cloudstats.cloudstats_01
where
--  date >= date_format(date_add('day', -65, to_timestamp('{{ get_batch_id(ts) }}'), '%Y-%m-%d')
  date >= date_format(date_add('day', -65, current_date), '%Y-%m-%d')
  and line_item_type in ('SavingsPlanCoveredUsage', 'DiscountedUsage', 'Usage')
  and usage > 0
  and cost > 0
;
