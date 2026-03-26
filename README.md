# dbt — webDataELT

DuckDB-backed dbt project for media dimension ELT, with unit tests via [dbt-unit-testing](https://github.com/EqualExperts/dbt-unit-testing).

## Setup

1. **Profile** — Copy `profiles.example.yml` to `profiles.yml` in this directory and set `outputs.dev.path` to your DuckDB database file (or `:memory:` for tests only).

2. **Python** — Use Python 3.9+ (dbt 1.10.x is tested here):

   ```powershell
   py -3.9 -m venv venv
   .\venv\Scripts\Activate.ps1
   pip install dbt-core dbt-duckdb
   ```

3. **Packages** — From the repo root:

   ```powershell
   $env:DBT_PROJECT_DIR = "$PWD\webDataELT"
   $env:DBT_PROFILES_DIR = "$PWD"
   dbt deps
   ```

4. **Tests**

   ```powershell
   dbt test --select test_type:unit
   ```

## Layout

- `webDataELT/` — dbt project (`dbt_project.yml`, models, tests)
- `profiles.yml` — **local only** (gitignored); use `profiles.example.yml` as a template

## What is not in git

`venv/`, `webDataELT/target/`, `webDataELT/dbt_packages/`, local `profiles.yml`, `.user.yml`, and `data/*.db` are ignored.
