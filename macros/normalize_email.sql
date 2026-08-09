-- Normalizes emails: trim + lowercase only, no aggressive dot/plus-tag stripping
-- Co-authored with CoCo

{% macro normalize_email(column_name) %}
    trim(lower({{ column_name }}))
{% endmacro %}
