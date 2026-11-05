-- Humana delivers claim lines by adjudication month (PROCESSED_DT) and stamps each
-- eligibility file with its as-of date (FILE_AS_OF_DT). Both must fall inside the
-- delivery month; a row here means a file landed in the wrong month's partition.
-- A claim can't be processed before it was received.

select 'medical_claim_lines' as model, claim_id as row_key, processed_date as checked_date,
       delivery_month, 'processed_date outside delivery month' as problem
from {{ ref('stg_humana__medical_claim_lines') }}
where date_trunc('month', processed_date) <> delivery_month

union all

select 'medical_claim_lines', claim_id, received_date, delivery_month,
       'received after processed'
from {{ ref('stg_humana__medical_claim_lines') }}
where received_date > processed_date

union all

select 'member_eligibility', member_id, file_as_of_date, delivery_month,
       'file_as_of_date outside delivery month'
from {{ ref('stg_humana__member_eligibility') }}
where date_trunc('month', file_as_of_date) <> delivery_month
