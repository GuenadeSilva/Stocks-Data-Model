select
    {{ dbt_utils.generate_surrogate_key(['date', 'symbol']) }} as primary_key,
    *
from {{ ref('int__financials_annual') }}
