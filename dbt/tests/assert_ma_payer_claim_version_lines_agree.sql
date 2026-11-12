-- Every line of an MA payer claim version must agree on the fields the version takes from
-- its lines with any_value() (int_ma_payer__claim_families: claim_versions). That rollup is
-- only correct when the lines agree; otherwise a version would take one arbitrary line's
-- status, form type, member, contract or link to its original. STOP: status and the link
-- decide the claim family and its net paid.
-- NULL counts as a value: any_value() skips NULLs, so a line missing a value that its
-- siblings carry would otherwise go unnoticed.

select claim_id, claim_frequency_code, delivery_month, _source_checksum, count(*) as line_count
from {{ ref('stg_ma_payer__medical_claim_lines') }}
group by claim_id, claim_frequency_code, delivery_month, _source_checksum
having count(distinct coalesce(original_claim_id, '<null>')) > 1
    or count(distinct coalesce(claim_status_code, '<null>')) > 1
    or count(distinct coalesce(claim_form_type_code, '<null>')) > 1
    or count(distinct coalesce(member_id, '<null>')) > 1
    or count(distinct coalesce(beneficiary_mbi, '<null>')) > 1
    or count(distinct coalesce(contract_id, '<null>')) > 1
