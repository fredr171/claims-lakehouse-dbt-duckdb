{#
    Incremental pattern shared by the intermediate models ("affected keys").

    Each intermediate row carries `_source_checksums`: the sorted SHA-256s of the raw
    files it was built from. On an incremental run:

      1. affected_keys() finds every key that has rows from a file not yet processed
         (a new delivery, or a corrected re-send of an old one), plus every key that
         was built from a file no longer in raw (a re-send replaced it).
      2. The model recomputes the FULL history of only those keys from staging, and
         delete+insert (unique_key = the key) replaces all of their rows at once.
      3. The post-hook delete_rows_from_vanished_files() removes keys whose source
         rows disappeared entirely, which delete+insert cannot see.

    Nothing assumes deliveries arrive in order, so an old month can be re-sent at any
    time. Recomputing a key's whole history is cheap (a beneficiary or claim family
    has a handful of rows) and keeps every run equal to a full rebuild; the dbt test
    suite checks that equivalence.
#}


{% macro row_hash(columns) -%}
    {#- md5 over the columns' text, with an explicit NULL marker so NULL <> ''. -#}
    md5(concat_ws('|'
    {%- for column in columns %},
        coalesce(cast({{ column }} as varchar), '<null>')
    {%- endfor %}
    ))
{%- endmacro %}


{% macro affected_keys(staged, key_column) -%}
    -- keys with rows from files this model has not processed yet
    select distinct {{ key_column }} as affected_key
    from {{ staged }}
    where _source_checksum not in (
        select distinct unnest(_source_checksums) from {{ this }}
    )

    union

    -- keys built from a file that is no longer in raw (replaced by a re-send)
    select {{ key_column }} as affected_key
    from {{ this }}
    where not list_has_all(
        (select list(distinct _source_checksum) from {{ staged }}),
        _source_checksums
    )
{%- endmacro %}


{% macro delete_rows_from_vanished_files(source_models) -%}
    {#- Post-hook. After delete+insert every recomputed key only references files that
        still exist, so anything left that references a vanished file belongs to a key
        with no source rows at all. -#}
    delete from {{ this }}
    where not list_has_all(
        (
            select list(distinct _source_checksum)
            from (
                {%- for model in source_models %}
                select _source_checksum from {{ model }}
                {%- if not loop.last %} union all{% endif %}
                {%- endfor %}
            )
        ),
        _source_checksums
    )
{%- endmacro %}
