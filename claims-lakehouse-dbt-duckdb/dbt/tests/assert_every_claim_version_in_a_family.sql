-- Every claim version in staging must be counted in exactly one claim family, per part.
-- A difference means an incremental run skipped (or double-counted) a family.

with staged as (

    select 'A' as part, count(distinct claim_id) as claim_versions
    from {{ ref('stg_cclf__claims_header') }}

    union all

    select 'B', count(distinct claim_id)
    from {{ ref('stg_cclf__part_b_physician_lines') }}

),

in_families as (

    select part, sum(version_count) as claim_versions
    from {{ ref('int_cclf__claim_families') }}
    group by part

)

select s.part, s.claim_versions as staged, f.claim_versions as in_families
from staged as s
left join in_families as f using (part)
where f.claim_versions is distinct from s.claim_versions
