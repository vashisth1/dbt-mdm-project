-- Generates a deterministic golden ID from a stable entity key
-- Co-authored with CoCo

{% macro generate_golden_id(entity_key_column) %}
    'G' || upper(substr(md5({{ entity_key_column }}), 1, 12))
{% endmacro %}
