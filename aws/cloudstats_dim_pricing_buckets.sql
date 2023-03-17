/**
This maps the cleaned up pricing_unit field made in stage 00 with less
granular pricing "buckets". Items in a bucket should generally be
comparable to each other. Eg, an hour on an EC2 instance is comparable
to an hour on an RDS or ELB instance. A GB-month is a GB-month; the
only difference is the rate you pay.

Bucketing items like this allows you to see your usage of the basic
resources you consume: Compute, Network, Storage, etc, across all
products.

I'm not 100% happy with this ontology. Monthly is only there to keep
out confusing spikes on timeseries graphs.

I'm also not happy with the Request bucket. As the years go on I see
AWS putting more and more usage into hard-to-compare a la carte requests.
**/
drop table if exists cloudstats.cloudstats_dim_pricing_buckets cascade;

create table cloudstats.cloudstats_dim_pricing_buckets (pricing_unit varchar primary key, pricing_bucket varchar);

insert into cloudstats.cloudstats_dim_pricing_buckets values
   ('CPU-Month',                'Monthly'),
   ('DNS-Month',                'Monthly'),
   ('Face-Month',               'Monthly'),
   ('Hardware-Month',           'Monthly'),
   ('Object-Month',             'Monthly'),
   ('SecurityCheck-Month',      'Monthly'),
   ('Software-Month',           'Monthly'),
   ('Support-Month',            'Monthly'),
   ('Tag-Month',                'Monthly'),
   ('User-Month',               'Monthly'),

   ('GB-Hour',                  'Storage'),
   ('GB-Month',                 'Storage'),
   ('Obj-Month',                'Storage'), -- todo: monthly? or best kept in Storage.
   ('UsageRecord-month',        'Storage'),

   ('Alarms',                   'Request'),
   ('ConfigRuleEvaluations',    'Request'),
   ('ConfigurationItemRecorded','Request'),
   ('Count',                    'Request'),
   ('Events',                   'Request'),
   ('HostedZone',               'Request'),  --todo: monthly?
   ('Keys',                     'Request'),
   ('Message',                  'Request'),
   ('Messages',                 'Request'),
   ('Metric Datapoints',        'Request'), -- Prometheus. todo: are these requests or processing? Not denominated in amt of data.
   ('Metrics',                  'Request'),
   ('Notifications',            'Request'),
   ('PutRequest',               'Request'),
   ('Request',                  'Request'),
   ('Secrets',                  'Request'),
   ('State Transitions',        'Request'),
   ('URL',                      'Request'),

   ('Hour',                     'Compute'),

   ('GB-Network',               'Network'),

   ('GB-IO',                    'IO'), -- S3, EFS
   ('GiBps-mo',                 'IO'),   -- EBS
   ('IOPS-Mo',                  'IO'),   --todo: better as monthly?
   ('IOs',                      'IO'),
   ('ReadCapacityUnit-Hrs',     'IO'),
   ('ReadRequestUnits',         'IO'),
   ('WriteCapacityUnit-Hrs',    'IO'),
   ('WriteRequestUnits',        'IO'),

   ('GB-Processed',             'Processing'), -- CloudTrail, etc
   ('GB-Second',                'Processing') -- Lambda
;

