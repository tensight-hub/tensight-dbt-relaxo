select * from {{ ref('review_across_channels') }} 
where scraped_date = 
(select max(scraped_date) as scraped_date from {{ ref('review_across_channels') }})