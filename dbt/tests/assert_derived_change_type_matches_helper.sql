-- The generator marks every CCLF8 row NEW or UPDATE (record_change_type, a synthetic
-- helper that real CCLF does not send). Change detection is derived independently in
-- int_cclf__beneficiary_versions; where the helper exists, both must agree:
-- every helper-flagged row starts a version, with the same NEW/UPDATE.
-- Real CCLF rows have no helper (NULL), so this test passes vacuously on them.

select
    s.beneficiary_mbi,
    s.delivery_month,
    s.record_change_type as helper_change_type,
    v.change_type        as derived_change_type
from {{ ref('stg_cclf__beneficiary_demographics') }} as s
left join {{ ref('int_cclf__beneficiary_versions') }} as v
    on v.beneficiary_mbi = s.beneficiary_mbi
    and v.valid_from = s.delivery_month
where s.record_change_type is not null
  and v.change_type is distinct from s.record_change_type
