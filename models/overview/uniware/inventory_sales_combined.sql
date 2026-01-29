
WITH InventoryDetails AS (
    SELECT  
        ib.*,
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
          a.available_atp as soh
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
      SELECT *, COUNT(*) AS units_sold 
      FROM {{ ref("stg_unicommerce_orders") }}
      GROUP BY 1,2,3,4,5,6,7,8,9,10,
               11,12,13,14,15,16,17,18,19,20,
               21,22,23,24,25,26,27,28,29,30,
               31,32,33,34,35,36,37,38,39,40,
               41,42,43,44,45,46,47,48,49,50,
               51,52,53
    ) so
    ON ib.sku_code = so.item_sku_code
    AND ib.facility_code = so.facility_code
    GROUP BY 1,2,3,4,5,6,7,8,9,10
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



