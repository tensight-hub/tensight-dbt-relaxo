{% macro channel_price(source_col, price_col, channel, alias) %}
    MAX(
        CASE WHEN lower({{ source_col }}) = '{{ channel | lower }}'
        THEN CAST(
            CASE WHEN lower({{ price_col }}) = 'nan' THEN '0' ELSE {{ price_col }} END
            AS DECIMAL(10, 2)
        )
    END
    ) AS {{ alias }}
{% endmacro %}
