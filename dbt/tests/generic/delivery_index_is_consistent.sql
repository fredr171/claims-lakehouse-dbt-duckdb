{#
    STOP (identity of what staging reads). A deliveries index (stg_<feed>__deliveries) is
    wrong if:
      - a delivery sits under another file's checksum folder (its partition checksum is
        not the SHA-256 recorded in its rows: a hand-copied or misplaced file), or
      - two deliveries of one file name tie for the latest ingested_at, so "current"
        would be a coin toss.
    Returns file names and checksums only (never row values).
#}
{% test delivery_index_is_consistent(model) %}

select 'stored under another checksum' as problem, file_type, file_name, checksum
from {{ model }}
where partition_checksum is distinct from checksum

union all

select 'tie for the latest delivery', file_type, file_name, min(checksum)
from {{ model }} d
where ingested_at = (
    select max(ingested_at) from {{ model }} m
    where m.file_type = d.file_type and m.file_name = d.file_name
)
group by file_type, file_name
having count(*) > 1

{% endtest %}
