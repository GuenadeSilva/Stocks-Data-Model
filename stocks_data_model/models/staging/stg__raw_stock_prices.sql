select *
from {{ source('stocks_model', 'raw_stock_prices') }}
