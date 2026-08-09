-- Builds a stable source_record_id qualifier of source_system + native id
-- Co-authored with CoCo

{% macro generate_source_record_id(source_system_column, native_id_column) %}
    {{ source_system_column }} || ':' || {{ native_id_column }}
{% endmacro %}
