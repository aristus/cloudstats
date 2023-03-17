/*
Total truncation and full load of data up to 2 years back. See _incremental_load.sql for the daily job.
*/

truncate table cloudstats.cloudstats;

insert into cloudstats.cloudstats
select * from cloudstats.cloudstats_01
where
--  date >= date_format(date_add('year', -2, to_timestamp('{{ get_batch_id(ts) }}'), '%Y-%m-%d')
  date >= date_format(date_add('year', -2, current_date), '%Y-%m-%d')
  and line_item_type in ('SavingsPlanCoveredUsage', 'DiscountedUsage', 'Usage')
  and usage > 0
  and cost > 0
;
