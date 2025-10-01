select all_data.* from 
{{ ref('uniware_sales_facility_date') }} all_data
inner join 
(select 
	facility_code, 
	sku_id,
	max(invoice_created) as latest_order_date 
	from 
	{{ ref('uniware_sales_facility_date') }} 
group by 1,2
) latest_data 
on all_data.facility_code = latest_data.facility_code 
and all_data.sku_id = latest_data.sku_id 
and all_data.invoice_created = latest_data.latest_order_date