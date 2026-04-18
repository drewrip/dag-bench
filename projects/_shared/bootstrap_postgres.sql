create schema if not exists runner;

create or replace function runner.date_diff(part text, start_value timestamp, end_value timestamp)
returns bigint
language sql
immutable
as $$
    select case lower(part)
        when 'year' then extract(year from end_value)::bigint - extract(year from start_value)::bigint
        when 'day' then floor(extract(epoch from (end_value - start_value)) / 86400)::bigint
        when 'hour' then floor(extract(epoch from (end_value - start_value)) / 3600)::bigint
        when 'minute' then floor(extract(epoch from (end_value - start_value)) / 60)::bigint
        when 'second' then floor(extract(epoch from (end_value - start_value)))::bigint
        else null
    end
$$;

create or replace function runner.date_diff(part text, start_value date, end_value date)
returns bigint
language sql
immutable
as $$
    select runner.date_diff(part, start_value::timestamp, end_value::timestamp)
$$;

create or replace function runner.date_diff(part text, start_value date, end_value timestamp)
returns bigint
language sql
immutable
as $$
    select runner.date_diff(part, start_value::timestamp, end_value)
$$;

create or replace function runner.date_diff(part text, start_value timestamp, end_value date)
returns bigint
language sql
immutable
as $$
    select runner.date_diff(part, start_value, end_value::timestamp)
$$;

create or replace function runner.strptime(value text, fmt text)
returns timestamp
language sql
immutable
as $$
    select case fmt
        when '%Y%m%d' then to_timestamp(value, 'YYYYMMDD')
        else null
    end
$$;

create or replace function runner.strftime(value timestamp, fmt text)
returns text
language sql
immutable
as $$
    select case fmt
        when '%Y%m%d' then to_char(value, 'YYYYMMDD')
        else null
    end
$$;

create or replace function runner.strftime(value date, fmt text)
returns text
language sql
immutable
as $$
    select runner.strftime(value::timestamp, fmt)
$$;
