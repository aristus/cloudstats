-- friendly names for your accounts. You can scrape this from the aws cli
-- or simply make up your own tags.
drop table if exists cloudstats.cloudstats_dim_account_names cascade;

create table cloudstats.cloudstats_dim_account_names (
  account_name varchar,
  account_nick varchar,
  account_id varchar primary key,
  account_owner varchar
);

insert into cloudstats.cloudstats_dim_account_names values
  -- eg ('Your Company Name, Inc', 'Production', '1234567890', 'sre-oncall@example.com'),
 
;
