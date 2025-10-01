select 
facility_code,
sku_id,
cast(sum(order_quantity) as double)/30 as drr_30d
from {{ ref('uniware_sales_facility_date') }} 
where invoice_created between 
CAST(date_add('day', -30, current_timestamp) AS date)
AND CAST(date_add('day',  -1, current_timestamp) AS date)
group by 1,2