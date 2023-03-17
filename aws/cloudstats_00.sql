/**
Stage 00 of our main pipeline. This is mostly about data cleanup. The AWS CUR is a combined
line-item bill and half of a system log unioned between dozens of AWS products written by
hundreds of people over nearly 20 years, stuffing product-specific info into shared fields
with only minimal reference to proper dataset design. Every log is different, with fields
appearing and disappearing, their meanings an values changing over time with many
adorable typos thrown in for fun.

NOTE: See redshift_athena_compat_udfs.sql for functions like if() and regexp_extract().
NOTE: Please see the where clause for notes on doing a full rebuild vs incremental.
**/

create or replace view cloudstats.cloudstats_00 as
select
  ----------------------------------------------
  -- BASE DIMENSIONS ---------------------------
  ----------------------------------------------
  year                                       as year,          -- '2022'
  month                                      as month,         -- '1' (note! not '01')
  date_format(date(line_item_usage_start_date), '%Y-%m-%d')
                                             as date,          -- '2022-01-01'
  date_trunc('week', date(line_item_usage_start_date))
                                             as date_week,     -- '2021-12-27' (the first Sunday in the week containing 2022-01-01)

  date_format(date(line_item_usage_start_date), '%Y-%m')
                                             as date_month,    -- '2022-01'

  date_format(date(line_item_usage_start_date), '%w (%a)')
                                             as day_of_week,   -- '1 (Sun)'

  '0.1.0'                                    as cloudstats_version,
  line_item_usage_account_id                 as account_id,
  bill_billing_entity                        as billing_entity,
  line_item_legal_entity                     as legal_entity,
  bill_invoice_id                            as invoice_id,
  --line_item_resource_id                      as resource_id, -- blows up cardinality

  /*
  resource_name: an artificial field. The resource_id is wonderful for diagnosis
  but too much for monitoring & analysis. Fortunately it has a handful of parsable
  patterns, so we can extract things like database name or elb pool.
  */
  coalesce(regexp_replace((case
    when line_item_resource_id = '' or line_item_resource_id is null
    then ''

    /* instances and volume ids. rewrite to the cluster name or other larger group. */
    when regexp_like(line_item_resource_id, '(i-[0-9a-f]{6,}|vol-[0-9a-f]{6,})|:instance/i-[0-9a-f]{6,}')
    then coalesce(
      /** ADD YOUR SITE-SPECIFIC TAGS HERE **/
       --if(resource_tags_user_cluster_name not in ('', ' '),  resource_tags_user_cluster_name),
       --if(resource_tags_user_name         not in ('', ' '),  resource_tags_user_name),
       null
    )

    /*
    Common colon-slash pattern. depending on the subservice, dial in the specificity.
      'arn:aws:rds:us-west-2:1234567890:subservice/foo/bar/baz'
      'arn:aws:logs:us-east-1:1234567890:log-group:/aws/eks/foobar/cluster'
    */
    when regexp_like(line_item_resource_id,':(?:awskms|cluster|crawler|directory|distribution|fargate|file-system|function|hostedzone|log-group|natgateway|storage-lens|table|task|userpool|workgroup|workspace)/')
    then regexp_extract(line_item_resource_id, ':([a-zA-Z0-9_\-]+:?/[a-zA-Z0-9_\-]+)', 1)

    /*
    Extract subservice/foo/bar
      'arn:aws:ecr:us-east-1:1234567890:repository/foo/bar' --> repository/foo/bar
    */
    when regexp_like(line_item_resource_id, ':(?:loadbalancer|repository)/')
    then regexp_extract(line_item_resource_id, ':([a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+)', 1)

    /*
    What about lovelies like this? they forgot their slashes :(
      'arn:aws:rds:us-west-2:1234567890:db:foo-bar-baz'
    */
    when regexp_like(line_item_resource_id,    '^arn:[a-zA-Z0-9_\-]+:[a-zA-Z0-9_\-]+:[a-zA-Z0-9_\-]+:[0-9]+:.+$')
    then regexp_extract(line_item_resource_id, '^arn:[a-zA-Z0-9_\-]+:[a-zA-Z0-9_\-]+:[a-zA-Z0-9_\-]+:[0-9]+:(.+)$', 1)

    /* Everything else, eg S3 bucket names. Can explode cardinality! */
    else line_item_resource_id
  end),
  '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', -- blank out UUIDs
  '<UUID>'), '')                             as resource_name,

  line_item_line_item_type                   as line_item_type,

  /* Clean up the cardinality of usage_type. Strip off regions, niggly usage details, etc. */
  coalesce(
    regexp_extract(line_item_usage_type, '^.*(?:SpotUsage:|InstanceUsage:|BoxUsage:|EBSOptimized:)(.+)', 1),
    regexp_extract(line_item_usage_type, '((IN|OUT)-Bytes-(Internet|AWS))$', 1),
    regexp_extract(line_item_usage_type, '^[A-Z]{2,}[0-9]?-[A-Z[0-9]]{2,}[0-9]?-(.+(In|Out)-Bytes)', 1),
    regexp_extract(line_item_usage_type, '^[A-Z]{3,}[0-9]-(.+)', 1),
    regexp_extract(line_item_usage_type, '^(US|ZA|SA|EU|AP|JP|CA|ME|IN|AU|NA)-(.+)', 2),
    regexp_extract(line_item_usage_type, '^(us|za|sa|eu|ap|jp|ca|me|in|au|na)-(east|west|central|north|south|northeast|northwest|southeast|southwest)-[0-9]-(.+)', 3),
    line_item_usage_type
  )                                          as usage_type,

  line_item_usage_type                       as usage_type_orig,
  product_region                             as region_code,
  product_location                           as location,

  /* CreateCacheCluster:0000:SV000 --> CreateCacheCluster */
  coalesce(
      regexp_extract(line_item_operation, '^([^:]+):(SV)?[0-9]+(:(SV)?[0-9]+)?$', 1),
      line_item_operation
  )                                          as operation,

  /* You'll thank me later. */
  line_item_currency_code                    as currency_code,

  /* Marketplace sometimes logs a crazy app id in the product code. */
  if(bill_billing_entity = 'AWS Marketplace', 'AWSMarketplace', line_item_product_code)
                                             as product_code,

  /* NOTE: This gets overlaid from a dim table in stage 01 */
  if(bill_billing_entity = 'AWS Marketplace', 'Marketplace', product_product_name)
                                             as product_name,

  if(bill_billing_entity = 'AWS Marketplace', 'Marketplace App', product_product_family)
                                             as product_family,

  if(bill_billing_entity = 'AWS Marketplace', product_product_name, product_group)
                                             as product_group,

  if(bill_billing_entity = 'AWS Marketplace', product_product_name, product_servicecode)
                                             as product_servicecode,

  /*
  Fix up the myriad of pricing_unit inconsistencies, in preparation for the pricing_bucket
  mapping in stage 01. Since this is a fallthrough statement against a high-cardinality field,
  the order matters. Most specific to most general.
  */
  (case
    /* workspaces, marketplace, etc. */
    when product_group_description = 'Billed by the month' and product_resource_type = 'Software' then 'Software-Month'
    when product_group_description = 'Billed by the month' and product_resource_type = 'Hardware' then 'Hardware-Month'
    when pricing_unit in ('Month', 'Months', 'month', 'months', 'Mo') and line_item_operation in ('GetLicense', 'Subscription') then 'Software-Month'

    /* AmazonCognito */
    when product_product_family = 'User Pool MAU' then 'User-Month'

    /*
    PROCESSING is like Networking, but the main differences are
      1: more intensive compute/transformation over the data, and
      2: the movement of data can be terminal (eg, ingestion)
    todo: how to make raw GB and GB-Second usages comparable?
    */
    when pricing_unit = 'GB' and line_item_product_code in ('AWSCloudTrail', 'AmazonCloudWatch', 'AmazonKinesisFirehose', 'AmazonSNS', 'AmazonGuardDuty', 'AmazonKinesis', 'AmazonTimestream', 'AWSShield', 'AmazonECRPublic', 'AmazonDynamoDB', 'AWSMarketplace') then 'GB-Processed'
    when pricing_unit in ('Lambda-GB-Second', 'Lambda-GB-Second-ARM', 'GB-Seconds') then 'GB-Second'
    when pricing_unit in ('Fargate-GB-Hours', 'GB-Hours', 'ECS-EC2-GB-Hours') then 'GB-Hour'
  
    /* AWS Athena. This is not storage, but the amount of bytes scanned. Usage is rescaled to GB below. */
    when pricing_unit = 'Terabytes' then 'GB-Processed'

    /*
    METERED IO from storage products, databases, etc.
    todo: does S3 GetObject qualify as processing? Or better if IO? The SQL-on-S3 stuff makes this uncertain.
    */
    when pricing_unit = 'GB' and line_item_product_code in ('AmazonS3', 'AmazonEFS') then 'GB-IO'

    /* NETWORK */
    when pricing_unit = 'GB' and product_product_family in ('Data Transfer', 'Load Balancer', 'VpcEndpoint', 'Lightsail Networking', 'Sending Attachments', 'NAT Gateway') then 'GB-Network'
    when pricing_unit = 'GigaBytes' then 'GB-Network' -- AWSGlobalAccelerator, AmazonVPC, etc
    -- todo: NatGateway-Bytes?

    /* MONTHLY charges. These are not amortized but could be with some work. */
    when pricing_unit = 'Month' and product_group = 'User' then 'User-Month'
    when pricing_unit = 'Faces-Mo' then 'Face-Month'
    when pricing_unit = 'Tag-Mo' then 'Tag-Month'
    when pricing_unit = 'Mo' and line_item_product_code = 'AmazonRoute53' then 'DNS-Month'
    when pricing_unit = 'Security Checks' then 'SecurityCheck-Month'
    when pricing_unit = 'vCPU-Months' then 'CPU-Month' -- RDS long term retention
    when pricing_unit in ('User', 'User-Mo') then 'User-Month'
    when pricing_unit = 'Objects' and line_item_product_code = 'AmazonS3' then 'Object-Month'
    when line_item_line_item_type = 'Fee' and pricing_unit in ('dollar', 'Dollar', 'dollars', 'Dollars') and line_item_product_code like '%Support%' then 'Support-Month'

    /* STORAGE, all products, including databases, volumes, s3, and so on. */
    when pricing_unit in ('GB-Mo', 'GB-month', 'GB-mo', 'GB-Month') then 'GB-Month'

    /* COMPUTE */
    when pricing_unit in ('Hrs', 'hrs', 'Hours', 'hours', 'hour', 'Hour', 'StreamHr', 'KPU-Hour', 'ACU-Hr', 'vCPU-Hours', 'LCU-Hrs', 'IdleLCU-Hr', 'DPU-Hour', 'Instance-hrs', 'Hourly', 'hourly', 'ShardHour', 'ConsumerShardHour', 'Accelerator-Hours', 'NatGateway-Hours', 'Rule-Hour') then 'Hour'
    when pricing_unit = 'Dashboards' and line_item_operation = 'DashboardHour' then 'Hour'

    /* nb: we also rescale the usage and rate below. */
    when pricing_unit in ('minute', 'minutes', 'Minute', 'Minutes', 'second', 'seconds', 'Second', 'Seconds') then 'Hour'

    /* REQUESTS (todo: rethink how to make these comparable to each other) */
    when pricing_unit in ('API Requests', 'Requests', 'Queries', 'FunctionExecutions') then 'Request'
    when pricing_unit like '%Request' then 'Request'

    --todo: how to bucket passthrough dollar charges from SMS?

    /*
    Like usage_type, pricing_unit has become a a dumping ground for
    region prefixes and inconsistencies. Eg, note the lack of a '-':

      USE1-AmazonApiGateway-Request
      USE2-AmazonApiGatewayRequest

    In these particular cases, the "like '%Request'" clause takes care of them above. But leave
    this here to catch new funny stuff that may come up in future.
    */
    when regexp_like(pricing_unit, '(US|ZA|SA|EU|AP|JP|CA|ME|IN|AU|NA)[EWNSC]?[0-9]?-.+')
    then regexp_extract(pricing_unit, '(?:US|ZA|SA|EU|AP|JP|CA|ME|IN|AU|NA)[EWNSC]?[0-9]?-(.+)')

    else pricing_unit
  end)                                       as pricing_unit,

  -- mess with a fundamental field this much, leave a way to debug it.
  pricing_unit                               as pricing_unit_orig,

  /*
  Specific to products with reservation/spot/etc options.
  todo: as of 1 Nov 2021 product_marketoption is undocumented, but sometimes has values when pricing_term does not.
  todo: I've yet to encounter a private rate applied to reserved/spot/etc but stranger things have happened.
  */
  (case
    when pricing_term = 'Reserved'        and line_item_line_item_type = 'DiscountedUsage'         then 'Reserved'
    when pricing_term = 'Spot'            and line_item_line_item_type = 'Usage'                   then 'Spot'
    when pricing_term in ('OnDemand', '') and discount_private_rate_discount != 0                  then 'PrivateRate'
    when pricing_term in ('OnDemand', '') and line_item_line_item_type = 'Usage'                   then 'OnDemand'
    when pricing_term in ('OnDemand', '') and line_item_line_item_type = 'SavingsPlanCoveredUsage' then 'SavingsPlan'
  else
    'Unknown'
  end)                                      as pricing_regime,

  /* useful for debug & diagnosis */
  coalesce(
    if(product_bundle_description      not in ('', ' '), product_bundle_description),
    if(product_description             not in ('', ' '), product_description),
    if(product_group_description       not in ('', ' '), product_group_description),
    if(line_item_line_item_description not in ('', ' '), line_item_line_item_description)
  )                                         as product_description,

  savings_plan_savings_plan_a_r_n           as savings_plan_arn,
  reservation_reservation_a_r_n             as reserved_instance_arn,

  /*
  ----------------------------------------------
  -- COMPUTE -----------------------------------
  ----------------------------------------------
  More data cleanup. non-EC2 products may use a type available to EC2, but add suffixes and prefixes that
  are (probably?) not relevant to the specs of the machine, eg "cache.m6g.large" or "g5.xlarge.search"
  so we make a new field called compute_instance_spec to normalize the types across aws products.

  The bet here is that instance types are SKUs and evolve slowly. Component drift may happen within a
  SKU, eg motherboard revs, but machines with the same "instance_spec" should have functionally
  equivalent performance and cost throughout their service life. This should hold true even if AWS
  were to virtualize classic machine specs on new hardware.

  However, over time, I expect non-EC2 services to use more and more specialized instance types
  for which spec info may not be available. Eg, "amazonsimpledb - standard". Even then they should
  have equivalent capability within a given SKU, though "capability" might be defined in terms of
  work throughput and not GB or GHz.

  It would be very interesting to collect standard benchmarks on the same machine specs over long
  periods of time.

  Why pay so much attention to this? Being able to compare roll-yer-own ElasticSearch performance to
  AWS OpenSearch on equivalent machines, for one. AWS's home-grown Graviton chips are becoming
  a major factor in cap planning, for another. Amazon has not (yet) completely abstracted hardware
  from billable usage, so nerding out on the hardware can yield better long-term costing decisions.

  See also cloudstats_dim_instance_specs.
  */
  lower(regexp_replace(product_instance_type, 'ml\.|\.search|db\.|cache\.|-Hosting|-Training|-TrainDebugFreeTier|-TrainDebug|-Notebook', ''))
                                             as compute_instance_spec,

  lower(product_instance_type)               as compute_instance_type,

  coalesce(
    if(product_instance_family not in ('', ' '), lower(product_instance_family)), -- regular EC2 instances
    if(product_bundle          not in ('', ' '), lower(product_bundle))           -- AmazonWorkSpaces
  )                                          as compute_instance_family,

  /*
  4 Oct 2021: Some non-EC2 products leave the product_instance_type_family null.
  This is probably from a missed join on c5.xlarge.search or whatever. >_<
  See also 01 stage for mapping based on instance_spec.
  */
  (case
    when product_instance_type_family = '' or product_instance_type_family is null
    then regexp_extract(lower(product_instance_type), '([a-z0-9]+)', 1)
    else lower(product_instance_type_family)
  end)                                       as compute_instance_type_family,

  (case
    when product_availability_zone is not null and product_availability_zone not in ('', 'NA')
    then product_availability_zone
    else line_item_availability_zone
  end)                                       as compute_availability_zone,

  product_capacitystatus                     as compute_capacity_status,
  product_physical_processor                 as compute_processor_name,

  /*
  Artificial fields: chip vendor / chip line.
  Some non-EC2 products leave product_physical_processor as null or ''. See also stage 01
  */
  (case
    when product_physical_processor is not null and product_physical_processor != ''
    then coalesce(regexp_extract(product_physical_processor, '(Intel|AMD|AWS)', 1), 'Unknown')
  end)                                       as compute_processor_vendor,

  (case
    when product_physical_processor is not null and product_physical_processor != ''
    then coalesce(regexp_extract(product_physical_processor, '((?:Intel|AMD|AWS) [^ ]+)', 1), 'Unknown')
  end)                                       as compute_processor_line,

  product_storage                            as compute_storage,
  product_operating_system                   as compute_os,

  /* see 'compute_software' in stage 01 */
  product_pre_installed_sw                   as compute_software,

  /*
  ----------------------------------------------
  -- STORAGE -----------------------------------
  ----------------------------------------------
  */
  (case
    --todo: onezone, etc
    when product_storage_class is null and line_item_usage_type = 'TimedStorage-ZIA-SmObjects' then 'Infrequent Access (Small Objects)'
    when line_item_usage_type = 'TimedStorage-INT-IA-ByteHrs'                                  then 'Intelligent (Infrequent Access)'
    when line_item_usage_type = 'TimedStorage-INT-AIA-ByteHrs'                                 then 'Intelligent (Archive Instant Access)'
    when line_item_usage_type = 'TimedStorage-INT-FA-ByteHrs'                                  then 'Intellegent (Frequent Access)'
    else product_storage_class
  end)                                       as storage_class,

  product_volume_type                        as storage_volume_type, -- SSD, Magnetic, etc
  product_volume_api_name                    as storage_volume_api,  --gp2, gp3, st1, etc
  product_uservolume                         as storage_user_volume,

  /*
  Thirty days hath September. This is needed to smooth out AWS's silly accounting
  for data storage in stage 01. TLDR: Amazon charges per "GB-Month", but defines every
  month as having 31 days. That means in February, your per-hour cost goes up by 10.7%.
  They accomplish this by inflating the *usage* amount. This is all carefully explained
  in your contract, though they don't go out of their way to highlight it. I suspect
  this is why every storage pricing example in the docs just happens to randomly
  choose a month that is 31 days long.

  Original algo by jcaesar, 15 Mar 708 anno urbis conditae
  Bugfixes by greg13@vatican.va, 15 Oct 1582 anno domini
  */
  (case
    when month in ('4', '6', '9', '11') then 30
    when month in ('1', '3', '5', '7', '8', '10', '12') then 31
    when cast(year as int) % 4 = 0 and not (cast(year as int) % 100 = 0 and cast(year as int) % 400 = 0) then 29
    else 28
  end)                                       as days_in_month,

  /*
  ----------------------------------------------
  -- ADD YOUR SITE-SPECIFIC TAGS HERE ----------
  ----------------------------------------------
  */
  -- resource_tags_my_tag                       as my_tag,

  /*
  ----------------------------------------------
  -- MEASURES ----------------------------------
  ----------------------------------------------
  */
  count(1)                                   as record_cnt,

  /*
  "true" usage
  Note the rescaling of some usage amounts, to make it easier to compare units of time or data.
  */
  sum(line_item_usage_amount / (
    case
      when pricing_unit in ('second', 'seconds', 'Second', 'Seconds') then 3600  -- to Hour
      when pricing_unit in ('minute', 'minutes', 'Minute', 'Minutes') then 60    -- to Hour
      when pricing_unit in ('Terabytes')                              then 0.0009765625 -- to GB-Processed
                                                                      else 1
    end))                                    as usage,

  /*
  "true" cost, essentially the amortized, accrual-basis cost of a lineitem's usage.
  this elides amortization / fees / "blending", and lump-sum charges like support.
  why this and not just blended_cost? Because we want to preserve information on the
  real rates paid for Spot/SP/RI and OnDemand.
  */
  sum((case line_item_line_item_type
    when 'Usage'                   then line_item_net_unblended_cost
    when 'DiscountedUsage'         then reservation_net_effective_cost
    when 'SavingsPlanCoveredUsage' then savings_plan_net_savings_plan_effective_cost
    else 0
  end))                                       as cost,

  /*
  same as "true" cost, but backing out EDP discounts. This is only really used in the
  _bill tables, to calculate the impact of separate discounting regimes. Note the use
  of *_effective_cost and not *_NET_effective_cost.

  todo: I've never seen a private rate applied to RI/SP, but it's possible...
  */
  sum((case line_item_line_item_type
    when 'Usage'                   then line_item_net_unblended_cost + discount_edp_discount
    when 'DiscountedUsage'         then reservation_effective_cost
    when 'SavingsPlanCoveredUsage' then savings_plan_savings_plan_effective_cost
    else 0
  end))                                      as cost_without_edp,

  sum(line_item_unblended_cost)              as unblended_cost,

  /*
  TIP: if you don't have an Enterprise Discount Program (EDP) agreement with AWS, edp_discount may not exist.
  ditto for private_rate_discount if you don't have a Private Pricing Addendum (PPA).
  */
  sum(discount_edp_discount)                 as edp_discount,
  sum(discount_private_rate_discount)        as private_rate_discount,
  sum(discount_total_discount)               as total_discount, -- edp + private rate + whatever new fields come in future

  -- the default ondemand public price, to compare the effect of spot/reserved/savingsplan/edp
  sum(pricing_public_on_demand_cost)         as public_cost

/*
TIP: create a sample table with only a day or a week of data, to speed up debugging. eg:

  create or replace table cur_sample as
    select * from {{ your_aws_cur_table }}
    where
      year='2023' and month='1'
      and line_item_usage_start_date between
        timestamp('2023-01-01') and timestamp('2023-01-07')
  ;

Random sampling of the table works as well, but because different services have different
logging rates the graphs will look very weird. An S3 bucket logs only one storage lineitem
per day per storage class, while every hour of every EC2 volume will log its own lineitem.
*/
from {{ your_aws_cur_table }}

where
  /*
  There are a *lot* of usage lineitems that are always free, even to the public.
  Since these activities are always free no matter how much you use, changing your
  usage does not move the cost needle. The main reason we exclude them is because
  there are separate lineitems for disk & network IO attached to EC2 instances.
  Unfortunately, these records are NEARLY INDISTINGUSHABLE FROM COMPUTER TIME. Like,
  they only differ from the actual computer time in informational fields like *_description.
  These lineitems realllly mess you up when calculating rates and summing instance-hours,
  and doing string parsing on usage_type is just asking for bugs later on.
  So, better to nuke them all from orbit. This may undercount usage, but not cost.
  */
  not (line_item_line_item_type = 'Usage' and pricing_public_on_demand_rate = '0.0000000000')

  /*
  months-back filter. the AWS CUR is partitioned by year and month, where month is NOT zero-padded.
  This twisty bit of logic takes the current date and filters on the current year/month partition
  plus the N year/month partitions prior to that. Uncomment if your views run too slow when doing
  incremental inserts.

  and (
    (year = to_char(to_timestamp(current_date, 'YYYY-MM-01'), 'YYYY')
    and month = cast(cast(to_char(to_timestamp(current_date, 'YYYY-MM-01'), 'MM') as int) as varchar))

    or (year = to_char(to_timestamp(current_date, 'YYYY-MM-01') - interval '1 month', 'YYYY')
    and month = cast(cast(to_char(to_timestamp(current_date, 'YYYY-MM-01') - interval '1 month', 'MM') as int) as varchar))

    or (year = to_char(to_timestamp(current_date, 'YYYY-MM-01') - interval '2 month', 'YYYY')
    and month = cast(cast(to_char(to_timestamp(current_date, 'YYYY-MM-01') - interval '2 month', 'MM') as int) as varchar))
    ...
  )
  */

--','.join([str(x+1) for x in range(NN)])
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50
