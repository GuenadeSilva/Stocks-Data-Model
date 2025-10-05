select *
from {{ source('stocks_model', 'raw_dividends') }}
