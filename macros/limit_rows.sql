{% macro limit_rows(limit_count=100) %}
    {% if target.name == 'dev' %}
        LIMIT {{ limit_count }}
    {% endif %}
{% endmacro %}
