select 
uniware_sales.master_mapping_channel_name,
uniware_sales.facility,
uniware_sales.sku_id,
uniware_sales.order_id,
uniware_sales.order_date,
uniware_sales.invoice_created,
uniware_sales.brand_sku_id,
uniware_sales.sku_category,
uniware_sales.sku_sub_category,
uniware_sales.name,
uniware_sales.total_price as total_amount,
uniware_sales.units_sold as total_quantity,
COALESCE(uniware_returns.total_price,0) as returned_amount,
COALESCE(uniware_returns.units_sold,0) as returned_quantity,
(uniware_sales.total_price - COALESCE(uniware_returns.total_price,0)) as net_amount,
(uniware_sales.units_sold - COALESCE(uniware_returns.units_sold,0)) as net_quantity

from {{ ref('int_uniware_sales_with_master_attributes') }} uniware_sales 
left join 
{{ ref('int_uniware_returns_with_master_attributes') }} uniware_returns
on uniware_sales.order_id = uniware_returns.order_id
and uniware_sales.sku_id = uniware_returns.sku_id