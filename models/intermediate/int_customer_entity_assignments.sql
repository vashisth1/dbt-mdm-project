-- Assigns each source record to a connected-component entity key using AUTO_MATCH edges,
-- resolving transitive matches across up to 3 source systems (bounded label propagation)
-- Co-authored with CoCo

with nodes as (
    select distinct
        source_system,
        source_record_id,
        source_system || ':' || source_record_id as node_id
    from {{ ref('int_customer_normalized') }}
),

edges as (
    select
        source_record_id_a_system || ':' || source_record_id_a as node_a,
        source_record_id_b_system || ':' || source_record_id_b as node_b
    from {{ ref('int_customer_match_decisions') }}
    where match_status = 'AUTO_MATCH'
    union all
    select
        source_record_id_b_system || ':' || source_record_id_b as node_a,
        source_record_id_a_system || ':' || source_record_id_a as node_b
    from {{ ref('int_customer_match_decisions') }}
    where match_status = 'AUTO_MATCH'
),

label_r0 as (
    select node_id, node_id as label from nodes
),

label_r1 as (
    select n.node_id, min(least(n.label, coalesce(e_lbl.label, n.label))) as label
    from label_r0 n
    left join edges e on e.node_a = n.node_id
    left join label_r0 e_lbl on e_lbl.node_id = e.node_b
    group by n.node_id, n.label
),

label_r2 as (
    select n.node_id, min(least(n.label, coalesce(e_lbl.label, n.label))) as label
    from label_r1 n
    left join edges e on e.node_a = n.node_id
    left join label_r1 e_lbl on e_lbl.node_id = e.node_b
    group by n.node_id, n.label
),

label_r3 as (
    select n.node_id, min(least(n.label, coalesce(e_lbl.label, n.label))) as label
    from label_r2 n
    left join edges e on e.node_a = n.node_id
    left join label_r2 e_lbl on e_lbl.node_id = e.node_b
    group by n.node_id, n.label
)

select
    {{ generate_golden_id('l.label') }} as golden_id,
    n.source_system,
    n.source_record_id,
    l.label as entity_key
from nodes n
join label_r3 l on l.node_id = n.node_id
