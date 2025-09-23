select 
	uniware_returns.master_mapping_channel_name,
	uniware_returns.facility,
	uniware_returns.item_sku_code as sku_id,
	uniware_returns.sale_order_code as order_id,
	uniware_returns.invoice_created,
    uniware_returns.order_date,
	uniware_returns.sale_order_status as order_status,
	uniware_returns.total_price,
	1 as units_sold,
	sku_master.brand_sku_id,
	sku_master.sku_category,
	sku_master.sku_sub_category,
	sku_master.name
from 
(select * from {{ ref('stg_unicommerce_orders') }} 
	where sale_order_status = 'COMPLETE'
	and (return_date is not null or reverse_pickup_created_date is not null)) as uniware_returns
left join {{ ref('stg_product_master') }} as sku_master
on uniware_returns.item_sku_code = sku_master.sku_item_code
and lower(uniware_returns.master_mapping_channel_name) = lower(sku_master.channel)
