/*
Wrapper functions for compatibility with certain Athena/Trino/Presto functions.

Cloudstats is written to be as SQL-neutral as possible, but there are funny differences that
can't be avoided. Fortunately, Redshift supports simple scalar UDFs so we can fake most of
it with the sleazy tricks below.

NOTE: there is no way to make an aggregate UDF in Redshift, so the approx_percentile()
calls in cloudstats_cpu_ratecard.sql has some commented out Athena code.
*/


/**
  if(predicate, true_case [, false_case])

  Be aware that this does not do lazy evaluation of the second or third arguments!
**/
create or replace function if (boolean, varchar, varchar) returns varchar immutable as $$ select (case when $1 then $2 else $3 end) $$ language sql;
create or replace function if (boolean, float, float)     returns float   immutable as $$ select (case when $1 then $2 else $3 end) $$ language sql;
create or replace function if (boolean, int, int)         returns int     immutable as $$ select (case when $1 then $2 else $3 end) $$ language sql;
create or replace function if (boolean, varchar)          returns varchar immutable as $$ select (case when $1 then $2 end) $$ language sql;
create or replace function if (boolean, float)            returns float   immutable as $$ select (case when $1 then $2 end) $$ language sql;
create or replace function if (boolean, int)              returns int     immutable as $$ select (case when $1 then $2 end) $$ language sql;

/**
  regexp_extract(haystack, pattern [, ignored_arg])

  Implements Trino-compatible regexp_extract() in Redshift. Returns null on failure.
  This IGNORES the third argument, which in Trino allows you to specify the group to
  capture. Redshift's regexp_substr() doesn't support that. You will have to be
  careful to use non-capturing groups, eg, "(?:  )" in your pattern because this
  wrapper function will ALWAYS return the first capturing group.

  For example:

  ## Trino:
    select regexp_extract('foobar', '(foo)(bar)', 1);
      --> 'foo'

    select regexp_extract('foobar', '(foo)(bar)', 2);      ## extracts second capturing group
      --> 'bar'

    select regexp_extract('foobar', '(?:foo)(bar)', 1);    ## non-capturing group on 'foo'
      --> 'bar'

  ## Redshift
    select regexp_extract('foobar', '(foo)(bar)', 1);
      --> 'foo'

    select regexp_extract('foobar', '(foo)(bar)', 2);      ## WRONG!
      --> 'foo'

    select regexp_extract('foobar', '(?:foo)(bar)', 1);    ## non-capturing group on 'foo'
      --> 'bar'
**/
create or replace function regexp_extract(varchar, varchar, int) -- $3 is ignored.
  returns varchar immutable
as $$
  select nullif(regexp_substr($1, $2, 1, 1, 'pe'), '')
$$ language sql;

create or replace function regexp_extract(varchar, varchar)
  returns varchar immutable
as $$
  select regexp_extract($1, $2, 0) -- $3 is ignored.
$$ language sql;


/**
  regexp_like(haystack, pattern)

  Redshift's regexp_instr() returns the ones-indexed position of the matching substring,
  or 0 if no match. Trino's regexp_like() only returns true or false.
**/
create or replace function regexp_like(varchar, varchar) returns boolean immutable as $$
  select regexp_instr($1, $2, 1, 1, 1, 'p') > 0
$$ language sql;


/**
  date_format(date | timestamp, format)

  VERY SIMPLISTIC implementation of Athena/MySQL's date_format() in Redshift.
  only supports year, month, day, dayofweek and dayabbv.

  select date_format(current_date, '%Y-%m-%d')
    --> '2022-04-18'

  select date_format(current_date, '%w (%a)')
    --> '1 (Mon)'
**/
create or replace function date_format(timestamp, varchar) returns varchar immutable as $$
  select
    to_char($1,
    replace(
    replace(
    replace(
    replace(
    replace($2,
      '%Y', 'YYYY'),
      '%m', 'MM'),
      '%d', 'DD'),
      '%w', 'D'),
      '%a', 'dy')
    )
$$ language sql;

create or replace function date_format(date, varchar) returns varchar immutable as $$
  select date_format(cast($1 as timestamp), $2)
$$ language sql;


