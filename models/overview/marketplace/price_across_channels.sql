
SELECT
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    name,
    sku_size,
    sku_gender,
    tagging,
    max(image_url) AS image_url,

    {{ channel_price('source', 'product_price', 'amazon', 'amazon_price') }},
    {{ channel_price('source', 'product_price', 'flipkart', 'flipkart_price') }},
    {{ channel_price('source', 'product_price', 'myntra', 'myntra_price') }},
    {{ channel_price('source', 'product_price', 'Ajio', 'ajio_price') }},

    {{ channel_product_url('source', 'product_url', 'amazon', 'amazon_url') }},
    {{ channel_product_url('source', 'product_url', 'flipkart', 'flipkart_url') }},
    {{ channel_product_url('source', 'product_url', 'myntra', 'myntra_url') }},
    {{ channel_product_url('source', 'product_url', 'ajio', 'ajio_url') }},
    
    MAX(
        CASE WHEN lower(source) = 'amazon'
        THEN CAST(
            CASE WHEN lower(mrp) = 'nan' THEN '0' ELSE mrp END
            AS DECIMAL(10, 2)
        )
    END
    ) AS amazon_mrp,

    MAX(
        CASE WHEN lower(source) = 'flipkart'
        THEN CAST(
            CASE WHEN lower(mrp) = 'nan' THEN '0' ELSE mrp END
            AS DECIMAL(10, 2)
        )
    END
    ) AS flipkart_mrp,

    MAX(
        CASE WHEN lower(source) = 'myntra'
        THEN CAST(
            CASE WHEN lower(mrp) = 'nan' THEN '0' ELSE mrp END
            AS DECIMAL(10, 2)
        )
    END
    ) AS myntra_mrp,

    MAX(
        CASE WHEN lower(source) = 'ajio'
        THEN CAST(
            CASE WHEN lower(mrp) = 'nan' THEN '0' ELSE mrp END
            AS DECIMAL(10, 2)
        )
    END
    ) AS ajio_mrp

from 
{{ ref('int_buybox_rating_and_reviews') }}

GROUP BY
    name,
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    sku_size,
    tagging,
    sku_gender;
