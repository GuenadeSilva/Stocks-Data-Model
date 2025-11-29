select
    {{ dbt_utils.generate_surrogate_key(['quarter', 'symbol']) }} as primary_key,
    *
from {{ ref('int__quarterly_dividends') }}
