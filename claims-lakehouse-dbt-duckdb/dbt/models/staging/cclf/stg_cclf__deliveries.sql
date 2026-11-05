-- Every CCLF file delivered to raw, one row per delivery (immutable bronze): a corrected
-- re-send is a second row for the same file_name, and the latest ingested one is current.
-- The staging models read only current deliveries (macros/deliveries.sql). A table, not a
-- view: every staging model filters on it, so it is computed once per build.

{{ config(materialized='table') }}

{{ deliveries_index('cclf', [
    'claims_header',
    'claims_diagnosis',
    'part_b_physician_lines',
    'beneficiary_demographics',
]) }}
