select *
from {{ ref('gld_sales_by_region') }}
where total_revenue < 0
