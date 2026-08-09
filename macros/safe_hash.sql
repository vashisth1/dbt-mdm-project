-- Deterministic hash wrapper used for record/attribute fingerprints
-- Co-authored with CoCo

{% macro safe_hash(expression) %}
    md5(coalesce(cast({{ expression }} as varchar), '~NULL~'))
{% endmacro %}
