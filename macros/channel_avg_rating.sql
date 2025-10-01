
{% macro channel_avg_rating(source_col, rating_col, channel, alias) %}
    MAX(
        CASE WHEN lower({{ source_col }}) = '{{ channel | lower }}'
        THEN CAST(
            CASE WHEN lower({{ rating_col }}) = 'nan' THEN '0' ELSE {{ rating_col }} END
            AS DECIMAL(10, 2)
        )
    END
    ) AS {{ alias }}
{% endmacro %}