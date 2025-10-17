

{% macro categorize_product(product_name_col) %}
    CASE 
        WHEN lower({{ product_name_col }}) LIKE '%sandal%' THEN 'Sandals'
        WHEN lower({{ product_name_col }}) LIKE '%flip flop%' 
          OR lower({{ product_name_col }}) LIKE '%flip-flop%' 
          OR lower({{ product_name_col }}) LIKE '%flipflop%'
          OR lower({{ product_name_col }}) LIKE '%slip on%' 
          OR lower({{ product_name_col }}) LIKE '%slip-on%' THEN 'Flip Flops'
        WHEN lower({{ product_name_col }}) LIKE '%sneakers%' THEN 'Sneakers'
        WHEN lower({{ product_name_col }}) LIKE '%shoes%' 
          AND lower({{ product_name_col }}) NOT LIKE '%running%' 
          AND lower({{ product_name_col }}) NOT LIKE '%casual%' THEN 'Shoes'
        WHEN lower({{ product_name_col }}) LIKE '%running shoes%' THEN 'Running Shoes'
        WHEN lower({{ product_name_col  }}) LIKE '%casual shoes%' THEN 'Casual Shoes'
        WHEN lower({{ product_name_col }}) LIKE '%shoe%' THEN 'Shoes'
        WHEN lower({{ product_name_col }}) LIKE '%slipper%' THEN 'Slipper'
        WHEN lower({{ product_name_col }}) LIKE '%clogs%' THEN 'Clogs'
    END
{% endmacro %}