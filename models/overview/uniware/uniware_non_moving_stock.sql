SELECT 
    sku_code,
    facility_code,
    soh,
    units_sold_30,
    stock_date
    FROM {{ ref('inventory_sales_combined') }}
WHERE soh > 0
AND units_sold_30 = 0
