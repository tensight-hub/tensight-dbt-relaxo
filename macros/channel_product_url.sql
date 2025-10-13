{% macro channel_product_url(source_col, url_col, platform, alias) %}
    MAX(CASE WHEN {{ source_col }} = '{{ platform }}' THEN {{ url_col }} END) AS {{ alias }}
{% endmacro %}