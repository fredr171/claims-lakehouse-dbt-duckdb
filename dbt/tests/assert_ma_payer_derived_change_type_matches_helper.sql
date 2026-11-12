-- The generator marks every MAP_ELIG row NEW, UPDATE or TERM (record_change_type, a
-- synthetic helper a real payer would not send). int_ma_payer__member_versions derives
-- the change type independently; where the helper exists, both must agree: every
-- helper-flagged row starts a version with the same type. Passes vacuously without it.

select
    s.member_id,
    s.delivery_month,
    s.record_change_type as helper_change_type,
    v.change_type        as derived_change_type
from {{ ref('stg_ma_payer__member_eligibility') }} as s
left join {{ ref('int_ma_payer__member_versions') }} as v
    on v.member_id = s.member_id
    and v.valid_from = s.delivery_month
where s.record_change_type is not null
  and v.change_type is distinct from s.record_change_type
