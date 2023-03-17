-- a quick pass over which products and resources can be purchased under which buying options

drop table if exists cloudstats.cloudstats_buying_options cascade;
create table cloudstats.cloudstats_buying_options (
  product_code varchar,
  pricing_bucket varchar,
  buy_options varchar
);
insert into cloudstats.cloudstats_buying_options values
  ('AmazonEC2',         'Compute', 'Reserved, SavingsPlan, Spot, ProvisionedIO'),
  ('AmazonRDS',         'Compute', 'Reserved'),
  ('AmazonES',          'Compute', 'Reserved'),
  ('AmazonElastiCache', 'Compute', 'Reserved'),
  ('AWSLambda',         'Compute', 'SavingsPlan'),
  ('AWSFargate',        'Compute', 'SavingsPlan'),
  ('AmazonRedshift',    'Compute', 'Reserved'),
  ('AmazonSageMaker',   'Compute', 'SavingsPlan'),
  ('AmazonDynamoDB',    'IO',      'ProvisionedIO')
;

drop view if exists cloudstats.cloudstats_buying_efficiency cascade;
create view cloudstats.cloudstats_buying_efficiency as

/* --athena
with cloudstats_buying_options as (
  select * from (
  values
    row('AmazonEC2',         'Compute', ARRAY['Reserved', 'SavingsPlan', 'Spot', 'ProvisionedIO']),
    row('AmazonRDS',         'Compute', ARRAY['Reserved']),
    --row('AWSELB',            'Compute', ARRAY['OnDemand']),
    row('AmazonES',          'Compute', ARRAY['Reserved']),
    row('AmazonElastiCache', 'Compute', ARRAY['Reserved']),
    row('AWSLambda',         'Compute', ARRAY['SavingsPlan']),
    row('AWSFargate',        'Compute', ARRAY['SavingsPlan']),
    row('AmazonRedshift',    'Compute', ARRAY['Reserved']),
    row('AmazonSageMaker',   'Compute', ARRAY['SavingsPlan']),
    row('AmazonDynamoDB',    'IO',      ARRAY['ProvisionedIO'])
  ) tmp (product_code, pricing_bucket, buy_options)
)
*/

select
  a.date_month,
  a.product_code,
  a.pricing_regime,
  a.pricing_bucket,
  a.pricing_unit,
  --athena: array_join(b.buy_options, ', ') as buy_options,
  b.buy_options,
  cast(sum(a.cost) as int) as cost
from
  cloudstats.cloudstats a inner join cloudstats.cloudstats_buying_options b
    on a.product_code = b.product_code
    and a.pricing_bucket = b.pricing_bucket
where
  a.date >= date_format(date_add('month', -3, current_date), '%Y-%m')
  and a.pricing_regime = 'OnDemand'
group by 1,2,3,4,5,6
order by 7 desc;

