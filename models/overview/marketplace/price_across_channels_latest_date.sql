select * from {{ ref('price_across_channels') }} 
where scraped_date = 
(select max(scraped_date) as scraped_date from {{ ref('price_across_channels') }})


