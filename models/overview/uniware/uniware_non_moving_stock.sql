SELECT 
    sku_code,
    facility_code,
    type,
    soh,
    units_sold_30,
    units_sold_60,
    units_sold_90,
    stock_date
    FROM {{ ref('uniware_inventory_sales_report') }}
