SELECT *
FROM {{ ref('rating_across_channels') }}
WHERE DATE_TRUNC('month', CAST(scraped_date AS DATE)) = (
    SELECT DATE_TRUNC('month', MAX(CAST(scraped_date AS DATE)))
    FROM {{ ref('rating_across_channels') }}
);

