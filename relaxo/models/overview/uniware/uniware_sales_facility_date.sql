{{ config(
    partitioned_by=['invoice_created'],
    insert_overwrite=True
) }}


select facility,
facility_code,
sku_id,
brand_sku_id,
sku_category,
sku_sub_category,
name,
order_amount,
order_quantity,
invoice_created from 
(select 
facility,
facility_code,
sku_id,
brand_sku_id,
sku_category,
sku_sub_category,
name,
sum(total_price) as order_amount,
sum(units_sold) as order_quantity,
invoice_created
from 
{{ ref('int_uniware_sales_with_master_attributes') }}
group by 1,2,3,4,5,6,7,10) x 
