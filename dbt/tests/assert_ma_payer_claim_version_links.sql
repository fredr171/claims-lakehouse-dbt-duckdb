-- How an MA payer claim line says what kind of version it is:
--   original     frequency 1, no original_claim_id, status P (paid) or D (denied)
--   replacement  frequency 7, original_claim_id set, new claim_id
--   reversal     frequency 8, original_claim_id = its own claim_id, status R
-- Downstream claim families group on coalesce(original_claim_id, claim_id), so a line
-- breaking these rules would land in the wrong family or none.

select claim_id, line_number, claim_frequency_code, claim_status_code, original_claim_id,
       delivery_month
from {{ ref('stg_ma_payer__medical_claim_lines') }}
where (claim_frequency_code = '1' and (original_claim_id is not null or claim_status_code not in ('P', 'D')))
   or (claim_frequency_code = '7' and (original_claim_id is null or original_claim_id = claim_id))
   or (claim_frequency_code = '8' and (original_claim_id is distinct from claim_id or claim_status_code <> 'R'))
   or (claim_status_code = 'R' and claim_frequency_code <> '8')
