# Résumé bullets for this project

Add under **PROJECTS**. Swap the GitHub link and tune numbers to what you actually run.

### Version A — concise (2 bullets)

**Mortgage Lending Data Platform** — *Apache Spark, Airflow, Snowflake/Redshift, Python*  ·  github.com/swati-murmu/mortgage-lending-data-platform
- Built an end-to-end pipeline processing 20M+ HMDA mortgage loan-application records with PySpark, orchestrated by Airflow with a data-quality gate, retries, and daily scheduling
- Modeled a star-schema warehouse (fact + 4 conformed dimensions) and authored window-function analytics for denial-rate, top-lender, and YoY-growth reporting

### Version B — detailed (for the project's own section)

**Mortgage Lending Analytics Data Platform** — *PySpark, Apache Airflow, Snowflake/Redshift, Parquet, DuckDB, pytest*
- Engineered a distributed, medallion-architecture pipeline (raw → curated → marts → warehouse) processing 20M+ mortgage records; all transforms run in Spark and scale to 100M+ rows
- Applied partitioning (by year/state) and broadcast joins to prune scans and eliminate shuffles when building the fact table
- Implemented a hard data-quality gate that blocks the warehouse load on assertion failure, plus pytest coverage of transform logic runnable in CI without a cluster
- Designed a star schema and window-function SQL (RANK, LAG) for denial-rate-by-geography and lender YoY-growth analytics

---

## How to talk about it in interviews

- **"Why this scale?"** → "The pipeline never assumes the data fits in memory — generation and every transform are distributed Spark, partitioned so common filters prune. I've run it at 20M and parameterized to 100M+."
- **"Why a star schema?"** → conformed dimensions, surrogate keys, and why fact grain = one application.
- **"What breaks at scale?"** → shuffle on the geography join (mitigated with partitioning), skew on large lenders, small-file problem on write (mitigated with partition sizing).
- **Tie to your day job** → this is the same mortgage/correspondent-lending domain you already work in, so the domain answers are real, not memorized.

## Before you push to GitHub
1. `git init && git add . && git commit -m "Initial commit: mortgage lending data platform"`
2. Create the repo on GitHub, then `git remote add origin ...` and push.
3. Run `make run ROWS=2000000` once and paste a screenshot of the KPI output into the README (recruiters love seeing real output).
4. Optional: add a GitHub Actions workflow running `make test` so the repo shows a green CI badge.
