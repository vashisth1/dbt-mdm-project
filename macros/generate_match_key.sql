-- Generates a deterministic blocking/match key by hashing a pipe-delimited concatenation of normalized fields
-- Co-authored with CoCo

{% macro generate_match_key(fields) %}
    md5(concat_ws('|', {{ fields | join(', ') }}))
{% endmacro %}
