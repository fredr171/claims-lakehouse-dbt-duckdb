{#
    Payer registry helpers: the marts are payer-agnostic and learn about payers only from
    the `payers` var (dbt_project.yml).

    Every registered payer provides two conformed views with the SAME contract (columns and
    types enforced by dbt; models/intermediate/_conformed__models.yml):
      int_<dbt_source>__conformed_coverage   one row per coverage span of a payer member
      int_<dbt_source>__conformed_claims     one row per claim family
    Payer-specific knowledge (code values, statuses, how coverage is delivered) lives in
    those views; the marts only union them.
#}


{% macro payer_registry() -%}
    {#- The validated `payers` var: unique snake_case ids and sources, YYYY-MM months. -#}
    {%- set payers = var('payers', []) -%}
    {%- if payers | length == 0 -%}
        {{ exceptions.raise_compiler_error("var 'payers' is empty: register at least one payer") }}
    {%- endif -%}
    {%- set seen_ids, seen_sources = [], [] -%}
    {%- for p in payers -%}
        {%- for key in ['payer_id', 'payer_name', 'line_of_business', 'dbt_source', 'onboarded_month'] -%}
            {%- if not p.get(key) -%}
                {{ exceptions.raise_compiler_error("payer " ~ p ~ " is missing " ~ key) }}
            {%- endif -%}
        {%- endfor -%}
        {%- if not modules.re.fullmatch('[a-z][a-z0-9_]*', p.payer_id)
               or not modules.re.fullmatch('[a-z][a-z0-9_]*', p.dbt_source) -%}
            {{ exceptions.raise_compiler_error("payer_id and dbt_source must be snake_case: " ~ p.payer_id) }}
        {%- endif -%}
        {%- for key in ['onboarded_month', 'retired_month'] -%}
            {%- if p.get(key) and not modules.re.fullmatch('\d{4}-(0[1-9]|1[0-2])', p.get(key) | string) -%}
                {{ exceptions.raise_compiler_error(p.payer_id ~ ": " ~ key ~ " must be YYYY-MM") }}
            {%- endif -%}
        {%- endfor -%}
        {%- if p.payer_id in seen_ids or p.dbt_source in seen_sources -%}
            {{ exceptions.raise_compiler_error("duplicate payer_id or dbt_source: " ~ p.payer_id) }}
        {%- endif -%}
        {%- do seen_ids.append(p.payer_id) -%}
        {%- do seen_sources.append(p.dbt_source) -%}
    {%- endfor -%}
    {{ return(payers) }}
{%- endmacro %}


{% macro is_registered(dbt_source) -%}
    {#-
        True if a payer with this dbt_source is in the registry. Conformed views use it as
        `enabled`, so removing a payer from the registry (offboarding) disables its conformed
        views and drops it from every mart; its staging and intermediate history stays.
    -#}
    {{ return(dbt_source in (payer_registry() | map(attribute='dbt_source') | list)) }}
{%- endmacro %}


{% macro payer_id(dbt_source) -%}
    {#- The registered payer_id of a feed, as a SQL string literal (conformed views use it). -#}
    {%- set ids = payer_registry() | selectattr('dbt_source', 'equalto', dbt_source)
                                   | map(attribute='payer_id') | list -%}
    {%- if ids | length == 1 -%}
        '{{ ids[0] }}'
    {%- elif execute -%}
        {{ exceptions.raise_compiler_error("no registered payer for dbt_source " ~ dbt_source) }}
    {%- else -%}
        'unregistered'  {#- parse time only: the model is disabled and never runs -#}
    {%- endif -%}
{%- endmacro %}


{% macro built_payer_relations(model_prefix, model_suffix) -%}
    {#-
        [{payer, relation}] for every registered payer whose model
        <model_prefix><dbt_source><model_suffix> exists in the warehouse.

        Every ref is declared at parse time, so the DAG always includes every payer. At run
        time a payer that has not built yet (just onboarded, no file delivered) is left
        out instead of failing the marts of the payers that have data.
    -#}
    {%- set found = [] -%}
    {%- for p in payer_registry() -%}
        {%- set relation = ref(model_prefix ~ p.dbt_source ~ model_suffix) -%}
        {%- if not execute or load_relation(relation) is not none -%}
            {%- do found.append({'payer': p, 'relation': relation}) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(found) }}
{%- endmacro %}


{% macro union_conformed(kind) -%}
    {#- UNION ALL BY NAME of every built payer's int_<src>__conformed_<kind> view. -#}
    {%- set parts = built_payer_relations('int_', '__conformed_' ~ kind) -%}
    {%- if execute and parts | length == 0 -%}
        {{ exceptions.raise_compiler_error("no payer has built int_*__conformed_" ~ kind ~ " yet") }}
    {%- endif -%}
    {%- for part in parts %}
    select * from {{ part.relation }}
    {%- if not loop.last %}
    union all by name
    {%- endif -%}
    {%- endfor %}
{%- endmacro %}
