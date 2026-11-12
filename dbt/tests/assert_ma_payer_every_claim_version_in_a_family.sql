-- Every MA payer claim version (claim_id + frequency code) and every paid dollar in
-- staging must be counted in exactly one claim family. A difference means an
-- incremental run skipped (or double-counted) a family.

with staged as (

    select
        count(distinct (claim_id, claim_frequency_code)) as claim_versions,
        sum(paid_amount)                                  as paid_amount
    from {{ ref('stg_ma_payer__medical_claim_lines') }}

),

in_families as (

    select sum(version_count) as claim_versions, sum(net_paid_amount) as paid_amount
    from {{ ref('int_ma_payer__claim_families') }}

)

select s.claim_versions as staged_versions, f.claim_versions as family_versions,
       s.paid_amount as staged_paid, f.paid_amount as family_paid
from staged as s
cross join in_families as f
where f.claim_versions is distinct from s.claim_versions
   or f.paid_amount is distinct from s.paid_amount
