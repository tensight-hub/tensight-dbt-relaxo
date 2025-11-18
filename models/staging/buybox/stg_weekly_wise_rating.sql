/*with source as (
  select *
  from {{ source('buybox_overview', 'rating_across_channels_latest_month') }}
),
renamed as (
  select 
scraped_date,
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
amazon_url,
flipkart_url,
myntra_url,
ajio_url,
max(image_url) AS image_url
from source
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16



)
select * from renamed;

*/


with source as (
  select *
  from {{ source('buybox_overview', 'rating_across_channels_latest_month') }}
),
renamed as (
  select distinct
        scraped_date,
        --date_trunc('week', date_parse(scraped_date, '%Y-%m-%d')) AS week_start,
       -- date_add('day', -1, date_trunc('week', date_add('day', 1, date_parse(scraped_date, '%Y-%m-%d')))) AS week_start,
        date_trunc('week', date_add('day', 1, date_parse(scraped_date, '%Y-%m-%d'))) - interval '1' day AS week_start,
        relaxo_sku,
        tagging,
        sku_category,
        sku_sub_category,
        sku_size,
        sku_gender,
        amazon_avg_rating,
       flipkart_avg_rating,
       myntra_avg_rating,
       ajio_avg_rating,
        image_url,
        max(amazon_url)  OVER () AS amazon_url,
    max(flipkart_url) OVER () AS flipkart_url,
    max(myntra_url)   OVER () AS myntra_url,
    max(ajio_url)     OVER () AS ajio_url
    
    FROM source

    )
select * from renamed;
