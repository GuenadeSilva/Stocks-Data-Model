select
    date(date) as date,
    symbol,
    `Total Revenue` as total_revenue,
    ebitda,
    ebit,
    `Net Income` as net_income,
    `Gross Profit` as gross_profit,
    {{ current_ts() }} as etl_timestamp
from {{ ref('stg__raw_financials_quarterly') }}
