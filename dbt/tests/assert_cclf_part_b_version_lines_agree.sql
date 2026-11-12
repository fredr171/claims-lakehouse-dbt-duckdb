-- Every CCLF5 line of a Part B claim version must agree on the fields the version takes
-- from its lines with any_value() (int_cclf__claim_families: part_b_versions). Otherwise a
-- version would take one arbitrary line's beneficiary or adjustment type. STOP: the
-- adjustment type decides the claim family's status and net paid, and the MBI its member.
-- NULL counts as a value: any_value() skips NULLs.

select claim_control_number, claim_id, delivery_month, _source_checksum, count(*) as line_count
from {{ ref('stg_cclf__part_b_physician_lines') }}
group by claim_control_number, claim_id, delivery_month, _source_checksum
having count(distinct coalesce(beneficiary_mbi, '<null>')) > 1
    or count(distinct coalesce(adjustment_type_code, '<null>')) > 1
