select
    d.quarter_end_date as quarter,
    div.symbol,
    div.dividend_amount,
    {{ current_ts() }} as etl_timestamp
from {{ ref('stg__raw_dividends') }} as div
left join {{ ref('dim_date') }} as d on date(div.date) = d.date
