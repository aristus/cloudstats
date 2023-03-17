/**
Get the per-CPU-hour real rates paid on EC2 compute, broken down by pricing regime and processor.

NB: per-CPU rates are not perfect for aggregating across the many hundreds of instance types. But at
scale, and presuming the mix of special features like GPUs, SSDs, and high-click-speed chips doesn't
skew the averages too much, it's good enough for procurement work.

**/
create or replace view cloudstats.cloudstats_cpu_ratecard as
select
  date,
  -- eg, "AWS Graviton2: r6g"
  coalesce(compute_processor_line, 'Unknown') || ': ' || coalesce(compute_instance_type_family, '??') as vendor_family,

  compute_os,
  compute_software,
  compute_instance_type_family,
  pricing_regime,
  coalesce(compute_processor_vendor, 'Unknown') as compute_processor_vendor,
  sum(usage) as instance_hours,
  sum(cost) as cost,
  sum(usage * compute_processor_vcpu) as cpu_hours, --todo: default to 0, or 1, or leave null?
  sum(cost) / sum(usage * compute_processor_vcpu) as rate,
  sum(cost) / sum(usage * compute_processor_vcpu) * 100 as rate_cents,

  /* Athena */
  --athena:approx_percentile(cost / (usage * compute_processor_vcpu), 0.25) * 100 as rate_cents_p25,
  --athena:approx_percentile(cost / (usage * compute_processor_vcpu), 0.50) * 100 as rate_cents_p50,
  --athena:approx_percentile(cost / (usage * compute_processor_vcpu), 0.75) * 100 as rate_cents_p75,
  --athena:approx_percentile(cost / (usage * compute_processor_vcpu), 0.95) * 100 as rate_cents_p95

  /* Redshift */
  percentile_cont(0.25) within group (order by cost / (usage * compute_processor_vcpu)) * 100 as rate_cents_p25,
  percentile_cont(0.50) within group (order by cost / (usage * compute_processor_vcpu)) * 100 as rate_cents_p50,
  percentile_cont(0.75) within group (order by cost / (usage * compute_processor_vcpu)) * 100 as rate_cents_p75,
  percentile_cont(0.95) within group (order by cost / (usage * compute_processor_vcpu)) * 100 as rate_cents_p95

from cloudstats.cloudstats
where
  -- only look at compute costs, to exclude things like attached disks & network
  pricing_bucket = 'Compute'

  --todo: it's possible that we can get good insight by looking at other products
  -- with reserved and SP options, but for now let's keep it simple.
  and product_code = 'AmazonEC2'
  --and date >= date_format(date_add('year', -1, to_timestamp('{{ get_batch_id(ts) }}'), '%Y-%m-%d')
  and date >= date_format(date_add('year', -1, current_date), '%Y-%m-%d')

group by 1,2,3,4,5,6,7
order by 1 desc


