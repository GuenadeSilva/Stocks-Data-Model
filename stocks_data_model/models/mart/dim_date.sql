{% set start_date = '1987-01-01' %}
{% set end_date = '2035-12-31' %}

with date_spine as (
    select date
    from unnest(generate_date_array(
        date('{{ start_date }}'),
        date('{{ end_date }}'),
        interval 1 day
    )) as date
),

enhanced as (
    select
        date,
        extract(year from date) as calendar_year,
        extract(quarter from date) as calendar_quarter,
        extract(month from date) as calendar_month,
        format_date('%B', date) as month_name,
        extract(day from date) as day_of_month,
        extract(dayofweek from date) as day_of_week,
        format_date('%A', date) as day_name,
        -- ISO week definitions
        extract(isoweek from date) as iso_week,
        extract(isoyear from date) as iso_year,
        -- Week and weekend logic
        coalesce(extract(dayofweek from date) in (1, 7), false) as is_weekend,

        -- Month boundaries
        date_trunc(date, month) as month_start_date,
        date_sub(date_add(date_trunc(date, month), interval 1 month), interval 1 day) as month_end_date,

        -- Quarter boundaries
        date_trunc(date, quarter) as quarter_start_date,
        date_sub(date_add(date_trunc(date, quarter), interval 3 month), interval 1 day) as quarter_end_date,

        -- Year boundaries
        date_trunc(date, year) as year_start_date,
        date_sub(date_add(date_trunc(date, year), interval 1 year), interval 1 day) as year_end_date,

        -- Fiscal logic (example: fiscal year starts in April)
        case
            when extract(month from date) >= 4 then extract(year from date)
            else extract(year from date) - 1
        end as fiscal_year,
        case
            when extract(month from date) between 4 and 6 then 1
            when extract(month from date) between 7 and 9 then 2
            when extract(month from date) between 10 and 12 then 3
            else 4
        end as fiscal_quarter,
        case
            when extract(month from date) >= 4 then extract(year from date)
            else extract(year from date) - 1
        end || '-Q'
        || case
            when extract(month from date) between 4 and 6 then 1
            when extract(month from date) between 7 and 9 then 2
            when extract(month from date) between 10 and 12 then 3
            else 4
        end as fiscal_quarter_label,

        -- Flags
        coalesce(date = current_date(), false) as is_today,
        coalesce(date = date_add(current_date(), interval -1 day), false) as is_yesterday,
        coalesce(date = date_add(current_date(), interval 1 day), false) as is_tomorrow
    from date_spine
)

select * from enhanced
