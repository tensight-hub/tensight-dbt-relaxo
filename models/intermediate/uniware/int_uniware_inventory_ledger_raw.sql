SELECT 
  uni_inv.sku_code,
  uni_inv.facility_code,

  sku_master.brand_sku_id, 
  sku_master.sku_category,
  sku_master.sku_sub_category,
  sku_master.name,
  sku_master.sku_size,
  sku_master.sku_gender, 
  uni_inv.total_inventory,
  uni_sales.units_sold,
  uni_returns.units_returned,
  uni_inv.stock_date

FROM (
  SELECT 
  sku_code, 
  facility_code, 
  available_atp as total_inventory,
  stock_date
  FROM {{ ref('stg_unicommerce_inventory') }}
  WHERE "type" = 'GOOD_INVENTORY'
  group by 1,2,3,4
) AS uni_inv


LEFT JOIN (
select 
  facility_code,
  item_sku_code as sku_id,
  order_date,
  invoice_created,
  count(*) as units_sold
  FROM {{ ref('stg_unicommerce_orders') }} 
  where sale_order_status = 'COMPLETE'
  group by 1,2,3,4
) AS uni_sales 
  ON uni_inv.sku_code = uni_sales.sku_id 
  AND uni_inv.facility_code = uni_sales.facility_code
 -- AND uni_inv.stock_date = uni_sales.order_date

LEFT JOIN (
select 
  facility_code,
  item_sku_code as sku_id,
   order_date,
  count(*) as units_returned
  FROM {{ ref('stg_unicommerce_orders') }} 
  where sale_order_status = 'COMPLETE'
  and (return_date is not null or reverse_pickup_created_date is not null)
  group by 1,2,3
) AS uni_returns 
  ON uni_inv.sku_code = uni_returns.sku_id 
  AND uni_inv.facility_code = uni_returns.facility_code
 -- AND uni_inv.stock_date = uni_returns.order_date

LEFT JOIN (
  SELECT DISTINCT 
  brand_sku_id,
  sku_category,
  sku_sub_category,
  sku_item_code,
  name,
  sku_size,
  sku_gender
  FROM {{ ref('stg_product_master') }}
) AS sku_master 
  ON uni_inv.sku_code = sku_master.sku_item_code 