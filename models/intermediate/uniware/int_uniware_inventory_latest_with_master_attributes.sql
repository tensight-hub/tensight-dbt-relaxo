SELECT distinct 
  a.sku_code,
  a.facility_code,
  b.brand_sku_id, 
  b.sku_category,
  b.sku_sub_category,
  b.name,
  b.sku_size,
  b.sku_gender,
  a.available_atp as total_inventory,
  a.stock_date

FROM (
  SELECT * 
  FROM {{ ref('stg_unicommerce_inventory') }}
  where stock_date in (select max(stock_date)
    from {{ ref('stg_unicommerce_inventory') }})
  AND "type" = 'GOOD_INVENTORY'
) AS a


LEFT JOIN (
  SELECT DISTINCT * 
  FROM {{ ref('stg_product_master') }}
) AS b 
  ON a.sku_code = b.sku_item_code
    



