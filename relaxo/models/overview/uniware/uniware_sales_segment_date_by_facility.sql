{{ config(
    partitioned_by=['invoice_created'],
    insert_overwrite=True
) }}

select 
master_mapping_channel_name as platform,
facility_code,
sku_id,
sum(units_sold) as order_quantity,
invoice_created
from 
{{ ref('int_uniware_sales_with_master_attributes') }}
group by 1,2,3,5