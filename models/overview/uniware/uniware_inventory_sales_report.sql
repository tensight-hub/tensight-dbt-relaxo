/*WITH InventoryDetails AS (
    SELECT  
        ib.*,
        so.created_date,
        SUM(so.units_sold) AS units_sold,
        SUM(
          CASE
            WHEN so.created_date >= DATE_ADD('day', -30, CURRENT_DATE)
            THEN so.units_sold ELSE 0
          END
        ) AS units_sold_30,
        
        COALESCE(
          ROUND(
            SUM(
              CASE
                WHEN so.created_date >= DATE_ADD('day', -30, CURRENT_DATE)
                THEN so.units_sold ELSE 0
              END
            ) / 30.0, 
          2), 
        0) AS ros_30
    FROM
    (
      SELECT  
          a.sku_code,
          a.facility_code,
          a.type,
          b.sku_category,
          b.sku_sub_category,
          b.name,
          b.sku_size,
          b.sku_gender,
          a.stock_date,
          SUM(a.available_atp) AS soh
      FROM {{ ref("stg_unicommerce_inventory") }} AS a
      LEFT JOIN {{ ref("stg_product_master") }} AS b
        ON a.sku_code = b.sku_relaxo
      WHERE a.stock_date = (SELECT MAX(stock_date) FROM {{ ref("stg_unicommerce_inventory") }})
      GROUP BY 1,2,3,4,5,6,7,8,9
    ) ib
    LEFT JOIN (
      SELECT 
          created_date,
          item_sku_code,
          facility_code,
          COUNT(*) AS units_sold 
      FROM {{ ref("stg_unicommerce_orders") }}
      GROUP BY 1,2,3
    ) so
      ON ib.sku_code = so.item_sku_code
     AND ib.facility_code = so.facility_code
    GROUP BY 
        ib.sku_code, ib.facility_code, ib.type, ib.sku_category, 
        ib.sku_sub_category, ib.name, ib.sku_size, ib.sku_gender, 
        ib.stock_date, ib.soh, so.created_date
)

SELECT 
    *,
    ROUND(
      CASE 
        WHEN ros_30 = 0 THEN 0
        ELSE soh / ros_30
      END, 2
    ) AS doh,
    
    CASE 
      WHEN (ros_30 * 45) - soh < 0 THEN 0
      ELSE ROUND((ros_30 * 45) - soh, 2)
    END AS inventory_recommendation,
    
    CASE
      WHEN soh / ros_30 > 90 THEN 'overstock'
      WHEN soh / ros_30 BETWEEN 45 AND 90 THEN 'optimal'
      WHEN soh / ros_30 < 45 THEN 'understock'
      ELSE 'N/A'
    END AS status
FROM InventoryDetails;
*/

/*
WITH InventoryDetails AS (
    SELECT  
        ib.*,
        so.created_date,
        SUM(so.units_sold) AS units_sold,
        SUM(
          CASE
            WHEN so.created_date >= DATE_ADD('day', -30, CURRENT_DATE)
            THEN units_sold ELSE 0
          END
        ) AS units_sold_30,

        SUM(
         CASE
    WHEN so.created_date >= DATE_ADD('day', -60, CURRENT_DATE)
    THEN units_sold ELSE 0
  END
) AS units_sold_60,

SUM(
  CASE
    WHEN so.created_date >= DATE_ADD('day', -90, CURRENT_DATE)
    THEN units_sold ELSE 0
  END
) AS units_sold_90,
        
        COALESCE(
          ROUND(
            SUM(
              CASE
                WHEN so.created_date >= DATE_ADD('day', -30, CURRENT_DATE)
                THEN units_sold ELSE 0
              END
            ) / 30.0, 
          2), 
        0) AS ros_30

    FROM
    (
      SELECT DISTINCT
          a.sku_code,
          a.facility_code,
          a.type,
          b.sku_category,
          b.sku_sub_category,
          b.name,
          b.sku_size,
          b.sku_gender,
          a.stock_date,
          a.available_atp AS soh
      FROM 
      ( 
        SELECT * 
        FROM {{ ref("stg_unicommerce_inventory") }}
        WHERE stock_date = (SELECT MAX(stock_date) FROM {{ ref("stg_unicommerce_inventory") }})
      ) AS a
      LEFT JOIN {{ ref("stg_product_master") }} AS b
      ON a.sku_code = b.sku_relaxo
      GROUP BY 1,2,3,4,5,6,7,8,9,10
    ) ib
    LEFT JOIN 
    (
      SELECT created_date, item_sku_code, facility_code, COUNT(*) AS units_sold 
      FROM {{ ref("stg_unicommerce_orders") }}
      GROUP BY 1,2,3
    ) so
    ON ib.sku_code = so.item_sku_code
    AND ib.facility_code = so.facility_code
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
SELECT 
    *,
    ROUND(
      CASE 
        WHEN ros_30 = 0 THEN 0
        ELSE soh / ros_30
      END,
    2) AS doh,

   CASE 
  WHEN (ros_30 * 45) - soh < 0 THEN 0
  ELSE ROUND((ros_30 * 45) - soh, 2)
END AS inventory_recommendation,

    CASE
        WHEN soh / ros_30 > 90 THEN 'overstock'
        WHEN soh / ros_30 BETWEEN 45 AND 90 THEN 'optimal'
        WHEN soh / ros_30 < 45 THEN 'understock'
        ELSE 'N/A' 
    END AS Status
FROM InventoryDetails;
*/


-- WITH InventoryDetails AS (
--     SELECT  
--         ib.*,
--         so.created_date,
--         SUM(so.units_sold) AS units_sold,
--         SUM(
--           CASE
--             WHEN so.created_date >= DATE_ADD('day', -30, CURRENT_DATE)
--             THEN so.units_sold ELSE 0  
--           END
--         ) AS units_sold_30,

--         SUM(
--          CASE
--     WHEN so.created_date >= DATE_ADD('day', -60, CURRENT_DATE)
--     THEN so.units_sold ELSE 0 
--   END
-- ) AS units_sold_60,

-- SUM(
--   CASE
--     WHEN so.created_date >= DATE_ADD('day', -90, CURRENT_DATE)
--     THEN so.units_sold ELSE 0  
--   END
-- ) AS units_sold_90

--     FROM
--     (
--       SELECT DISTINCT
--           a.sku_code,
--           a.facility_code,
--           a.type,
--           b.sku_category,
--           b.sku_sub_category,
--           b.name,
--           b.sku_size,
--           b.sku_gender,
--           b.sub_brand,
--           a.stock_date,
--           a.available_atp AS soh
--       FROM 
--       ( 
--         SELECT * 
--         FROM {{ ref("stg_unicommerce_inventory") }}
--         WHERE stock_date = (SELECT MAX(stock_date) FROM {{ ref("stg_unicommerce_inventory") }})
--       ) AS a
--       LEFT JOIN {{ ref("stg_product_master") }} AS b
--       ON a.sku_code = b.sku_relaxo
--       GROUP BY 1,2,3,4,5,6,7,8,9,10,11
--     ) ib
--     LEFT JOIN 
--     (
--       SELECT created_date, item_sku_code, facility_code, COUNT(*) AS units_sold 
--       FROM {{ ref("stg_unicommerce_orders") }}
--       GROUP BY 1,2,3
--     ) so
--     ON ib.sku_code = so.item_sku_code
--     AND ib.facility_code = so.facility_code
--     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
-- )
-- SELECT * FROM InventoryDetails;

WITH latest_inventory AS (
    SELECT
        a.sku_code,
        a.facility_code,
        a.type,
        -- b.sku_category,
        -- b.sku_sub_category,
        -- b.name,
        -- b.sku_size,
        -- b.sku_gender,
        -- b.sub_brand,
        a.stock_date,
        a.available_atp AS soh
    FROM {{ ref("stg_unicommerce_inventory") }} a
    -- LEFT JOIN stg_product_master b
    --     ON a.sku_code = b.sku_relaxo
    WHERE a.stock_date = (
        SELECT MAX(stock_date) FROM {{ ref("stg_unicommerce_inventory") }}
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

    FROM {{ ref("stg_unicommerce_orders") }}
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


