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
case when lower(product_name) like '%sandal%' then 'Sandals'
when lower(product_name) like '%flip flop%' then 'Flip Flops'
when lower(product_name) like '%sneakers%' then 'Sneakers'
when lower(product_name) like '%shoes%' then 'Shoes'
when lower(product_name) like '%running shoes%' then 'Running Shoes'
when lower(product_name) like '%casual shoes%' then 'Casual Shoes'
when lower(product_name) like '%shoe%'  then 'Shoes'
when lower(product_name)  like '%slipper%' then 'Slipper'
when lower(product_name) like '%clogs%' then 'Clogs'
when lower(product_name)  like '%slip on%'then 'slip on'
end as category

 
from source
)
select * from renamed;