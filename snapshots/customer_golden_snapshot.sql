-- Snapshot of the golden customer mart for point-in-time history tracking
-- Co-authored with CoCo

{% snapshot customer_golden_snapshot %}

{{
    config(
        target_schema='MDM_AUDIT',
        unique_key='golden_id',
        strategy='timestamp',
        updated_at='updated_ts',
    )
}}

select * from {{ ref('customer_golden') }}

{% endsnapshot %}
