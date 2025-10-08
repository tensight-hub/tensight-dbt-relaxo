WITH InventoryDetails AS (
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