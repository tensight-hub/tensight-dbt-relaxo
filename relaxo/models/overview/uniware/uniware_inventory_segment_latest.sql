{{ config(
    partitioned_by=['stock_date'],
    insert_overwrite=True
) }}

SELECT 
  sku_code,
  facility_code,
  sku_category,
  sku_sub_category,
  name,
  sku_size,
  sku_gender,
  SUM(total_inventory) AS soh,
 COALESCE(SUM(total_inventory), 0) AS total,
  stock_date

FROM {{ref('int_uniware_inventory_latest_with_master_attributes')}}

GROUP BY 
  sku_code,
  facility_code,
  sku_category,
  sku_sub_category,
  name,
  sku_size,
  sku_gender,
  stock_date