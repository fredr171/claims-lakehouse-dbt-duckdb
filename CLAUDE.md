# Claims ETL (repo root)

Portfolio repo with two parts:

| Path | What |
|---|---|
| `.` (root) | Synthetic payer test-file generators. CCLF (Medicare FFS): `generate_cclf.py`, `validate_cclf.py`, `cclf_common.py`, `output/YYYY-MM/`, `answer_key/`. Humana MA: `generate_humana.py`, `validate_humana.py`, `humana_common.py`, `output/humana/`, `answer_key/humana/`. Shared: `member_registry.py` (people, coverage spans, switchers, providers), `clinical_reference.py`, `validate_crosspayer.py`, `answer_key/crossover_members.csv`. Tests in `tests/`. See README.md. |
| `claims-lakehouse-dbt-duckdb/` | AWS lakehouse ETL that ingests the CCLF CSVs generated here (S3, Lambda, Terraform; dbt-duckdb next). Has its own CLAUDE.md with the full rules. |

## Rules for the generator

- Standard library only; output must stay deterministic for a given seed. Never
  iterate over sets when making random choices.
- Everything is synthetic: no real names, addresses, identifiers, PHI or PII.
- Both generators share the registry: always run them with the same `--seed`,
  `--start` and `--end`. Each uses its own rng stream (`seed`, `registry:<seed>`,
  `humana:<seed>`), so changing one generator's draws must not alter the other's
  output. A registry change alters both.
- After any change: regenerate both (`python generate_cclf.py --seed 42` and
  `python generate_humana.py --seed 42`), run `validate_cclf.py`, `validate_humana.py`
  and `validate_crosspayer.py`, and sweep several seeds; all checks must pass.
- A change to seed-42 CCLF output also changes the hardcoded counts in
  `claims-lakehouse-dbt-duckdb/tests/test_dbt_intermediate.py`; update and run it.
- `pytest` and `ruff` at the root cover only the generator (root pyproject.toml).

## Working in the lakehouse project

- Run its commands from inside `claims-lakehouse-dbt-duckdb/` (pytest, ruff,
  `python scripts/build_lambda.py`, `terraform -chdir=infra/terraform ...`, CLIs).
  `.env` is located from the working directory. dbt commands run from
  `claims-lakehouse-dbt-duckdb/dbt/` (its profiles.yml lives there).
- One shared virtualenv at the root `.venv`, with the lakehouse package installed
  editable: `.venv/Scripts/python -m pip install -e "claims-lakehouse-dbt-duckdb[dev,dbt]"`.
- Terraform local state lives in `claims-lakehouse-dbt-duckdb/infra/terraform/`;
  move it only together with the rest of that tree.
