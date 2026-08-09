-- Normalizes phone numbers to a 10-digit national number, stripping punctuation and country code
-- Co-authored with CoCo

{% macro normalize_phone(column_name) %}
    right(regexp_replace({{ column_name }}, '[^0-9]', ''), 10)
{% endmacro %}
