{% macro channel_product_url(source_col, url_col, platform, alias) %}
    MAX(CASE WHEN LOWER({{ source_col }}) = LOWER('{{ platform }}')
            THEN {{ url_col }}
        END
    ) AS {{ alias }}
{% endmacro %}
