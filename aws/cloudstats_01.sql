/**
Stage 01 of our main pipeline. This stage is generally meant to join against
useful dimension tables. No aggregation of the data from 00.
**/
create or replace view cloudstats.cloudstats_01 as


-- map in friendly AWS account names and nicks
with b as (
  select
    account_id,
    account_name,
    account_nick
  from cloudstats.cloudstats_dim_account_names
),

-- map the fixed-up pricing units to coarser-grained "buckets"
c as (
  select
    pricing_unit,
    pricing_bucket
  from cloudstats.cloudstats_dim_pricing_buckets
),

-- le sigh.
d as (
  select
    instance_spec,
    compute_class,
    processor_name,
    processor_vendor,
    processor_line,
    cast(vcpu as int)                         as processor_vcpu
  from cloudstats.cloudstats_dim_instance_specs
  group by 1,2,3,4,5,6
),

-- product names, sigh
e as (
  select
    product_code,
    product_name
  from cloudstats.cloudstats_dim_aws_products
)

select
  -- date field is last, for patitioning.
  year,
  month,
  date_month,  -- 2021-09
  day_of_week, -- 1 (Mon)
  cloudstats_version,
  a.account_id                                as account_id,
  coalesce(b.account_name, 'Unknown')         as account_name,
  coalesce(b.account_nick, 'Unknown') || ' (' || a.account_id || ')'
                                              as account_display_name,
  billing_entity,
  legal_entity,
  invoice_id,
  a.resource_name                             as resource_name,
  line_item_type,
  usage_type,

  -- the prefix can be useful for, eg, tracking UnusedBox
  regexp_extract(usage_type, '^([^:]+):', 1)  as usage_type_prefix,

  region_code,
  location,
  operation,
  currency_code,
  coalesce(c.pricing_bucket, 'Unknown')       as pricing_bucket,
  a.product_code                              as product_code,
  coalesce(e.product_name, a.product_name)    as product_name,

  -- In the final bill AWS moves (some!) networking into a separate product, AWSDataTransfer.
  -- Tax in the CUR lines appear with the AWSDataTransfer code, but Usage and EdpDiscount lines
  -- do not. So, we gently rewrite lineitems with the "correct" product as it appears in the bill.
  --
  -- BUT we want to make sure that the usage/accrual tables in the main cloudstats table attribute
  -- charges like network to the usage that triggered them, while at the same time ensure the
  -- cloudstats_bill dag replicates the jiggery-pokery in the official PDF bill. Two different
  -- ways to denote "product". See also the virtual "Savings Plans for Compute" product that
  -- is used to mark non-usage charges for SP, which are multiproduct and can't be attributed
  -- to an individual product like RIFee can.
  (case
    when a.product_code in ('AWSELB', 'AmazonEC2', 'AmazonApiGateway', 'AmazonECR') and (
      (product_family = 'Data Transfer') or (line_item_type = 'EdpDiscount' and usage_type like '%DataTransfer-%-Bytes%')
    ) then 'AWSDataTransfer'
    else a.product_code
  end)                                        as bill_product_code,

  (case
    when a.product_code in ('AWSELB', 'AmazonEC2', 'AmazonApiGateway', 'AmazonECR') and (
      (product_family = 'Data Transfer') or (line_item_type = 'EdpDiscount' and usage_type like '%DataTransfer-%-Bytes%')
    ) then 'AWS Data Transfer'
    else coalesce(e.product_name, a.product_name)
  end)                                        as bill_product_name,

  -- product_family sometimes has weird data dropouts.
  (case
    when product_family = '' or product_family is null then coalesce(c.pricing_bucket, 'Unknown')
    else product_family
  end)                                        as product_family,

  product_group,
  product_servicecode,
  a.pricing_unit                              as pricing_unit,
  pricing_regime,
  product_description,
  savings_plan_arn,
  reserved_instance_arn,

  compute_instance_spec,
  compute_instance_type,
  compute_instance_family,
  compute_instance_type_family,

  --todo: az information is its own deep rabbit hole.
  compute_availability_zone,
  compute_capacity_status,

  -- no coalesce() here. Strictly overwrite with the static instance_spec data.
  d.compute_class                             as compute_class, -- c5->c5, c5a->c5, etc.
  d.processor_name                            as compute_processor_name,
  d.processor_vendor                          as compute_processor_vendor,
  d.processor_line                            as compute_processor_line,
  d.processor_vcpu                            as compute_processor_vcpu,

  compute_storage, --todo: d.compute_storage?

  (case
    -- lots of non-EC2 services omit stuff like this. As of 2022 it's a decent bet that these
    -- things run Linux, but who knows what the future will bring.
    when c.pricing_bucket = 'Compute' and (compute_os = '' or compute_os is null) then 'Linux'
    else compute_os
  end)                                        as compute_os,

  -- per-hour cost can vary quite a bit with pre-installed software. This is basically the only
  -- field that can distinguish them.
  -- "Linux: SQL Std"
  -- "Windows: SQL Ent"
  (case
    when compute_software in ('NA', '') or compute_software is null then (case when c.pricing_bucket = 'Compute' and (compute_os = '' or compute_os is null) then 'Linux' else compute_os end)
    else (case when c.pricing_bucket = 'Compute' and (compute_os = '' or compute_os is null) then 'Linux' else compute_os end) || ': ' || compute_software
  end)                                        as compute_software,

  storage_class,
  storage_volume_type,
  storage_volume_api,
  storage_user_volume,
  days_in_month,

  /*
  ----------------------------------------------
  -- ADD YOUR SITE-SPECIFIC TAGS HERE ----------
  ----------------------------------------------
  */
  -- resource_tags_my_tag                       as my_tag,

  ----------------------------------------------
  -- MEASURES ----------------------------------
  ----------------------------------------------
  record_cnt,
  usage,

  /*
  Combine amortized cost with month-adjusted storage cost. For reconciling this
  dataset with the real bill, use unblended_cost instead. For the full crazy
  story, see "days_in_month" in stage 00.

  
   */
  (case
    when a.pricing_unit = 'GB-Month' then cost / 30.4375 * days_in_month
    else cost
  end)                                        as cost,

  (case
    when a.pricing_unit = 'GB-Month' then cost / 30.4375 * days_in_month
    else cost
  end) * 30.4375                              as cost_mrr,

  unblended_cost,
  cost_without_edp,
  edp_discount,
  total_discount,
  cast(private_rate_discount as float)        as private_rate_discount,
  public_cost,

  /*
  The realized rates for aggregated line items, after the jiggery-pokery
  in the previous stage to normalize timed usage like "seconds" and "minutes" to
  "hour", accrual-basis accounting of cost, rolling up by date or hour, etc.
  very often a product will have multiple rate tiers (0.05 for first 50GB,
  0.04 for 51-200 GB) that make it tricky to aggregate rates at an earlier stage.

  Note that we are NOT using the month-adjusted storage cost here. AWS makes their
  "every month is 31 days" math work out by inflating the usage in short months.
  */
  cost / (usage + 1)                          as rate,
  public_cost / (usage + 1)                   as public_rate,

  /*
  Speaking of inflated usage amounts...
  Using days_in_month, calculate total data under management.
  If you just sum(usage) where pricing_bucket='Storage', you will find that
  the amount stored will appear to jump 3-11% when crossing from one month
  to another. It will fool your anomaly detectors and confuse humans.
  Instead, rescale "usage" by the actual number of days in the month.
  */
  if(a.pricing_unit = 'GB-Month', usage * days_in_month, 0)
                                              as storage_total_gb,

  if(a.pricing_unit = 'GB-Month', usage * days_in_month / 1024, 0)
                                              as storage_total_tb,

  --partition / sort field
  date

from
  cloudstats.cloudstats_00 a
  left join b on a.account_id = b.account_id
  left join c on a.pricing_unit = c.pricing_unit
  left join d on a.compute_instance_spec = d.instance_spec
  left join e on a.product_code = e.product_code
