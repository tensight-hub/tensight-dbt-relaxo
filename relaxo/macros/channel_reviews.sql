{% macro channel_reviews(source_col, review_col, channel, alias) %}
    MAX(
        CASE WHEN lower({{ source_col }}) = '{{ channel | lower }}'
        THEN CAST(
            CASE WHEN lower({{ review_col }}) = 'nan' THEN '0' ELSE {{ review_col }} END
            AS DECIMAL(10, 2)
        )
    END
    ) AS {{ alias }}
{% endmacro %}
