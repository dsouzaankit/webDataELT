# webDataELT (SCD type 2, duckdb, nodeJs, dbt, Cursor)

DuckDB-backed dbt project for media dimension ELT, with unit tests via [dbt-unit-testing](https://github.com/EqualExperts/dbt-unit-testing). The repo also includes a Node.js scraper and SQL scripts that feed the same analytical model.

## Node.js web scraping

The scraper in `node_scrape/web_scrape.js` calls the site REST API and loads DuckDB (see script comments for concurrency and connection notes). It supports:

- **Wall posts** — ingest for specific wall / post URLs you configure.
- **Chat threads** — ingest for specific chat thread URLs so message media can be collected in the same pipeline.

Use this path when you need to refresh source data before running dbt models.

## SCD type 2 features

Dimension modeling uses **slowly changing dimension type 2** semantics so media attributes can change over time without losing history. The design includes:

- **Backfill** — load or reprocess historical periods so late-arriving rows land in the correct effective intervals.
- **`is_deleted` flag** — soft-delete signal when source content disappears or is retracted, without dropping prior facts.
- **Deleted reactivation** — if a previously deleted entity reappears, versioning can open a new current row instead of mutating history in place.
- **JSON blob checksum** — detect real change vs. no-ops when API payloads repeat, so spurious SCD2 rows are avoided when the logical record is unchanged.
- **History table** — companion history / `*_history` style storage (aligned with dbt models such as `dbt_media_dim_history`) stores full type-2 timelines for audit and point-in-time queries.

SQL building blocks for DDL/DML and trackers live under `sql_script/`; dbt implements transforms and tests under `webDataELT/`.

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
- `node_scrape/` — Node.js API scraper → DuckDB
- `sql_script/` — raw SQL (DDL/DML, origin / SCD helpers such as `media_origin_date_tracker.sql`)
- `profiles.yml` — **local only** (gitignored); use `profiles.example.yml` as a template

## What is not in git

`venv/`, `webDataELT/target/`, `webDataELT/dbt_packages/`, local `profiles.yml`, `.user.yml`, and `data/*.db` are ignored.
