-- Normalizes names: lowercase, trim, collapse whitespace, strip punctuation
-- Co-authored with CoCo

{% macro normalize_name(column_name) %}
    trim(regexp_replace(lower({{ column_name }}), '[^a-z0-9 ]', ''))
{% endmacro %}
