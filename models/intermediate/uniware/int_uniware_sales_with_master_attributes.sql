select 
	uniware_sales.master_mapping_channel_name,
	uniware_sales.channel_name,
	uniware_sales.facility,
	uniware_sales.facility_code,
	uniware_sales.item_sku_code as sku_id,
	uniware_sales.sale_order_code as order_id,
	uniware_sales.sale_order_status as order_status,
	uniware_sales.order_date,
	uniware_sales.total_price,
	uniware_sales.invoice_created,
	1 as units_sold,
	sku_master.sku_relaxo,
	sku_master.brand_sku_id,
	sku_master.sku_category,
	sku_master.sku_sub_category,
	sku_master.name
from {{ ref('stg_unicommerce_orders') }} uniware_sales
--where sale_order_status = 'COMPLETE'
left join {{ ref('stg_product_master') }} as sku_master 
on uniware_sales.item_sku_code = sku_master.sku_relaxo
and lower(uniware_sales.master_mapping_channel_name) = lower(sku_master.channel)

