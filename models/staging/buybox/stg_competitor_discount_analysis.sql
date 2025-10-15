with source as (
  select *
  from {{ source('buybox', 'competitor_discount_analysis') }}
),
renamed as (
  select    

"sr.no" as sr_no,
platform,
datetime,
lower(keyword) as keyword,
pincode,
product_id,
product_url,
product_name,
product_rank,
TRY_CAST(mrp AS DOUBLE) as mrp,
TRY_CAST(selling_price AS DOUBLE) as selling_price,
TRY_CAST(discount_percent AS DOUBLE) as discount_percent,
"product image link" as product_image_link,
 {{ categorize_product('product_name') }} as category

 
from source 
)
select * from renamed;