SELECT
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    name,
    sku_size,
    sku_gender,
    max(image_url) AS image_url,

MAX(CASE WHEN lower(source) = 'amazon' THEN CASE 
    WHEN lower(is_sold_out) = 'nan' THEN 'unknown'
    WHEN lower(is_sold_out) = 'true' THEN 'out of stock'
    WHEN lower(is_sold_out) = 'false' THEN 'in stock'
    ELSE 'unknown'END
    END
) AS amazon_stock_availability,

MAX(CASE WHEN lower(source) = 'flipkart' THEN CASE 
    WHEN lower(is_sold_out) = 'nan' THEN 'unknown'
    WHEN lower(is_sold_out) = 'true' THEN 'out of stock'
    WHEN lower(is_sold_out) = 'false' THEN 'in stock'
    ELSE 'unknown'END
    END
) AS flipkart_stock_availability,

MAX(CASE WHEN lower(source) = 'myntra' THEN CASE 
    WHEN lower(is_sold_out) = 'nan' THEN 'unknown'
    WHEN lower(is_sold_out) = 'true' THEN 'out of stock'
    WHEN lower(is_sold_out) = 'false' THEN 'in stock'
    ELSE 'unknown'END
    END
) AS myntra_stock_availability,

MAX(CASE WHEN lower(source) = 'ajio' THEN CASE 
    WHEN lower(is_sold_out) = 'nan' THEN 'unknown'
    WHEN lower(is_sold_out) = 'true' THEN 'out of stock'
    WHEN lower(is_sold_out) = 'false' THEN 'in stock'
    ELSE 'unknown'END
    END
) AS ajio_stock_availability,

    {{ channel_product_url('source', 'product_url', 'amazon', 'amazon_url') }},
    {{ channel_product_url('source', 'product_url', 'flipkart', 'flipkart_url') }},
    {{ channel_product_url('source', 'product_url', 'myntra', 'myntra_url') }},
    {{ channel_product_url('source', 'product_url', 'Ajio', 'ajio_url') }}

from 
{{ ref('int_buybox_rating_and_reviews') }}

GROUP BY
    name,
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    sku_size,
    sku_gender;
