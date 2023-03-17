-- used to recreate the official monthly AWS bill. Note that unlike the
-- main-sequence cloudstats table, this groups by month and includes
-- all line item types. There's also jiggery-pokery in the product_name.
-- It also consolidates various lineitems into a "Charges" line, as in
-- in the official invoice.

truncate table cloudstats.cloudstats_bill;

insert into cloudstats.cloudstats_bill
  select
    date_month,
    line_item_type,

   (case
      when line_item_type in ('Usage', 'SavingsPlanCoveredUsage', 'SavingsPlanRecurringFee', 'SavingsPlanUpfrontFee', 'RIFee', 'Refund', 'DiscountedUsage', 'Fee') then 'Charges'
      when line_item_type in ('SavingsPlanNegation') then 'Savings Plan'
      when line_item_type in ('Credit') then 'Credits'
      else line_item_type
    end)                                     as bill_line,

    account_id,
    account_name || ' (' || account_id || ')' as account_name, -- name, not nick as in the accrual tables.
    billing_entity,
    legal_entity,
    invoice_id,
    bill_product_name                       as product_name, -- see _01 stage for an explanation.
    bill_product_code                       as product_code,
    location,
    product_description,

    -- the bucket for non-charge bill_lines is usually Unknown, so thwack in the product_code.
    -- this is because pricing_unit is null for these lines.
    (case
      when pricing_bucket = 'Unknown' then product_code
      else pricing_bucket
    end)                                     as pricing_bucket,

    pricing_unit,
    pricing_regime,

    sum(cost)                                as accrual_cost, -- this is the "true" cost used in the usage/accrual tables in the main branch
    sum(usage)                               as usage,

    -- this is the proper cost field for billing reports. Sum this, group by date_month,
    -- bill_line, product_name, and invoice_id, and that's your basic bill summary.
    sum(unblended_cost)                      as unblended_cost,
    sum(cost_without_edp)                    as cost_without_edp,
    count(distinct date)                     as days_in_month,

    -- for calculating your total savings, including Spot, SP, RI, EDP, PRD, etc etc etc.
    -- this compared to sum(unblended_cost) is the overall scorecard for the entire effort.
    sum(public_cost)                         as public_cost,
    sum(public_cost) - sum(unblended_cost)   as total_savings

  from cloudstats.cloudstats_01
  where
--  date_month >= date_format(date_add('year', -2, to_timestamp('{{ get_batch_id(ts) }}'), '%Y-%m')
  date_month >= date_format(date_add('year', -2, current_date), '%Y-%m')
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;
