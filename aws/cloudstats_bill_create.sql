-- used to recreate the official monthly AWS bill. Note that unlike the
-- main-sequence cloudstats table, this groups by month and includes
-- all line item types. There's also jiggery-pokery in the product_name.
-- It also merges various lineitems into a "Charges" line as seen
-- in the official invoice.

drop table if exists cloudstats.cloudstats_bill cascade;

create table cloudstats.cloudstats_bill (
  date_month varchar,
  line_item_type varchar,
  bill_line varchar,
  account_id varchar,
  account_name varchar,
  billing_entity varchar,
  legal_entity varchar,
  invoice_id varchar,
  product_name varchar,
  product_code varchar,
  location varchar,
  product_description varchar,
  pricing_bucket varchar,
  pricing_unit varchar,
  pricing_regime varchar,
  accrual_cost float,
  usage float,
  unblended_cost float,
  cost_without_edp float,
  days_in_month int,
  public_cost float,
  total_savings float
)

compound sortkey(date_month)
;

