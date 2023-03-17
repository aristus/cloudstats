/*
This builds on the cloudstats_bill table by:
  1) adding iceberg_cost to show the effect of various classes of discount strategies
  2) creating artificial "Reserved Instances" bill_line records to show the savings from RI
  3) adding an iceberg_bill_line to break out interesting charges like Support
*/

create or replace view cloudstats.cloudstats_bill_iceberg as

with bill as (
  select
    date_month,
    bill_line,
    pricing_bucket,
    billing_entity,
    line_item_type,
    product_code,
    product_description,

    sum(accrual_cost)                        as accrual_cost,
    sum(unblended_cost)                      as unblended_cost,
    sum(public_cost)                         as public_cost

  from cloudstats.cloudstats_bill

    -- exclude AWS marketplace charges, which generally have no discounts
  where billing_entity = 'AWS'
  group by 1,2,3,4,5,6,7
),

discounted_usage as (
  select
    date_month,
    'Reserved Instances'                     as bill_line,
    pricing_bucket,
    billing_entity,
    'RiDiscount'                             as line_item_type,
    product_code,
    product_description,

    sum(accrual_cost)                        as accrual_cost,

    -- fun fact: EDP is applied *after* savings plans and RIs. This means that
    -- if you you use $2.00 worth of compute under an SP/RI that carries a 50%
    -- discount, then you use up $1.00 of the commitment. But under a 10% EDP,
    -- your net charged amount is only $0.90. So the effective savings from
    -- an RI must add back in the savings from edp, so you don't double count.
    --
    -- confusingly, "DiscountedUsage" is reserved instance usage.
    sum((case
      when line_item_type = 'DiscountedUsage'
      then -1 * (public_cost - cost_without_edp)
      else unblended_cost
    end))                                    as unblended_cost,
    sum(public_cost)                         as public_cost

  from cloudstats.cloudstats_bill
  where
    line_item_type in ('DiscountedUsage', 'RiVolumeDiscount')

    -- exclude AWS marketplace charges, which generally have no discounts
    and billing_entity = 'AWS'
  group by 1,2,3,4,5,6,7

),

unioned as (
  select * from bill
  union
  select * from discounted_usage
),

calced as (

  select
    cast(concat(date_month, '-01') as timestamp) as date_month, --needed to get QS to treat this as a proper date
    bill_line,
    pricing_bucket,
    billing_entity,
    line_item_type,
    product_code,
    product_description,

    public_cost,

    --"iceberg" cost:
    -- 1) all positive charges logged as the net "true" cost including discounts
    -- 2) all negative lines (credits, etc)
    -- makes for a pretty graph to show the effect of each discount type.
    (case
      -- negative values, sum of discounts
      when bill_line = 'Reserved Instances' then unblended_cost
      when line_item_type in ('EdpDiscount', 'PrivateRateDiscount', 'Credit', 'SavingsPlanNegation') then unblended_cost

      -- positive values, with all discounts applied
      when line_item_type in ('SavingsPlanCoveredUsage', 'DiscountedUsage', 'Usage') then accrual_cost
      when line_item_type in ('Tax') then unblended_cost
      else 0
    end)                                       as iceberg_cost,

    -- pull out some interesting things out of Charges as separate bill lines for the iceberg chart
    (case
      when product_code like '%Support%' then 'Support'
      when line_item_type = 'Credit'     then 'Credits'
      when bill_line = 'Charges'         then 'Net Charges'
      else bill_line
    end)                                       as iceberg_bill_line

  from unioned
),

final_cte as (
  select
    date_month,
    iceberg_bill_line,
    product_code,
    product_description,
    (case when iceberg_cost >= 0 then 'Net Charges' else 'Discounts' end) as iceberg_type,
    sum(iceberg_cost) as iceberg_cost
  from calced
  group by 1,2,3,4,5
)

select
  *,

  iceberg_cost / sum(iceberg_cost) over (partition by date_month, iceberg_type) as percent_of_type,
  abs(iceberg_cost) / sum(abs(iceberg_cost)) over (partition by date_month) as percent_of_total

from final_cte
where iceberg_cost != 0
order by 1 desc, 3 asc
;
