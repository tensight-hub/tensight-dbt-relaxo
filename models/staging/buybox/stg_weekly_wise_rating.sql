with source as (
  select *
  from {{ source('buybox_overview', 'rating_across_channels') }}
),
renamed as (
  select  


date_trunc('week', date_parse(scraped_date, '%Y-%m-%d')) AS week_start,
relaxo_sku,
sku_category,
sku_sub_category,
name,
sku_size,
sku_gender,
amazon_avg_rating,
flipkart_avg_rating,
myntra_avg_rating,
ajio_avg_rating,
max(image_url) AS image_url

from source
group by 1,2,3,4,5,6,7,8,9,10,11


)
select * from renamed;
