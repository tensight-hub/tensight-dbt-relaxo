-- SELECT 
--     sku_code,
--     facility_code,
--     type,
--     soh,
--     units_sold_30,
--     units_sold_60,
--     units_sold_90,
--     stock_date
--     FROM {{ ref('uniware_inventory_sales_report') }} 

WITH latest_inventory AS (
    SELECT
        a.sku_code,
        a.facility_code,
        a.type,
        b.sku_category,
        b.sku_sub_category,
        b.name,
        b.sku_size,
        b.sku_gender,
        b.sub_brand,
        a.stock_date,
        a.available_atp AS soh
    FROM {{ ref('stg_unicommerce_inventory') }} a
    LEFT JOIN {{ ref('stg_product_master') }} b
        ON a.sku_code = b.sku_relaxo
    WHERE a.stock_date = (
        SELECT MAX(stock_date) FROM {{ ref('stg_unicommerce_inventory') }}
    )
),

order_agg AS (
    SELECT
        item_sku_code AS sku_code,
        facility_code,

        COUNT(*) AS units_sold,

        COUNT(
            CASE
                WHEN created_date >= DATE_ADD('day', -30, CURRENT_DATE)
                THEN 1
            END
        ) AS units_sold_30,

        COUNT(
            CASE
                WHEN created_date >= DATE_ADD('day', -60, CURRENT_DATE)
                THEN 1
            END
        ) AS units_sold_60,

        COUNT(
            CASE
                WHEN created_date >= DATE_ADD('day', -90, CURRENT_DATE)
                THEN 1
            END
        ) AS units_sold_90

    FROM {{ ref('stg_unicommerce_orders') }}
    GROUP BY 1,2
),
base_data AS (
SELECT distinct
    i.sku_code,
    i.facility_code,
    i.type,
    i.stock_date,
    i.soh,
    COALESCE(o.units_sold, 0)     AS units_sold,
    COALESCE(o.units_sold_30, 0)  AS units_sold_30,
    COALESCE(o.units_sold_60, 0)  AS units_sold_60,
    COALESCE(o.units_sold_90, 0)  AS units_sold_90,
    ROW_NUMBER() OVER (PARTITION BY i.sku_code,i.facility_code ORDER BY i.sku_code,i.facility_code  ASC) rn
FROM latest_inventory i
LEFT JOIN order_agg o
    ON i.sku_code = o.sku_code
   AND i.facility_code = o.facility_code
),
final_data AS(
SELECT distinct
    sku_code,
    facility_code,
    type,
    stock_date,
    soh,
    case when rn = 1 then units_sold else 0 end AS units_sold,
    case when rn = 1 then units_sold_30 else 0 end AS units_sold_30,
    case when rn = 1 then units_sold_60 else 0 end AS units_sold_60,
    case when rn = 1 then units_sold_90 else 0 end AS units_sold_90
FROM base_data 
)
SELECT distinct
    sku_code,
    facility_code,
    type,
    stock_date,
    max(soh) soh,
    sum(units_sold) units_sold,
    sum(units_sold_30) units_sold_30,
    sum(units_sold_60) units_sold_60,
    sum(units_sold_90) units_sold_90
FROM final_data  
group by 1,2,3,4

