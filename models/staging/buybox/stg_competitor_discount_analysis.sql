WITH source AS (
  SELECT *
  FROM {{ source('buybox', 'competitor_discount_analysis') }}
),
renamed AS (
  SELECT    
    "sr.no" AS sr_no,
    platform,
    datetime,
    LOWER(keyword) AS keyword,
    pincode,
    product_id,
    product_url,
    product_name,
    product_rank,
    TRY_CAST(mrp AS DOUBLE) AS mrp,
    TRY_CAST(selling_price AS DOUBLE) AS selling_price,
    TRY_CAST(discount_percent AS DOUBLE) AS discount_percent,
    "product image link" AS product_image_link,
    {{ categorize_product('product_name') }} AS category
  FROM source
  WHERE LOWER(keyword) NOT IN ('flite','paragon','sparks')
)

SELECT * FROM renamed;
