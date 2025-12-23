# Healthcare Analytics Lab (OLTP → Star Schema)

This mini project shows a very common real-world problem:  
a normalized OLTP database works great for day-to-day operations, but analytics queries can get slow because they need lots of joins and heavy aggregations.

So we:

1. build the OLTP schema
2. generate ~10,000 rows per table (so performance differences are visible)
3. run baseline analytics queries on OLTP and capture performance
4. build a star schema in a `dw` schema
5. load the DW (ETL in SQL)
6. rerun the same analytics queries on the star schema and compare results

---

## What’s in this repo (quick map)

- `00_oltp_schema.sql` — creates the OLTP tables
- `03_generate_10k_balanced.sql` — generates ~10k rows per table (with a good date spread)
- `11_baseline_queries_v2.sql` — baseline (OLTP) versions of Q1–Q4
- `star_schema.sql` (or `20_star_schema.sql`) — creates the star schema tables in `dw`
- `30_load_dw.sql` — loads the star schema from OLTP (the ETL step)
- `40_star_queries.sql` — star schema versions of Q1–Q4
- `perf/`
  - `baseline_explain_v2/` — saved EXPLAIN outputs for baseline queries
  - `star_explain/` — saved EXPLAIN outputs for star schema queries

Deliverables (rubric files) at repo root:

- `query_analysis.txt`
- `design_decisions.txt`
- `star_schema.sql`
- `star_schema_queries.txt`
- `etl_design.txt`
- `reflection.md`

---

## What you need

- PostgreSQL installed locally
- `psql` works in your terminal

---

## Run it from scratch (step by step)

### 1) Create the database

```bash
createdb healthcare_oltp
```

````

Quick check:

```bash
psql -d healthcare_oltp -c "SELECT current_database();"
```

---

### 2) Create the OLTP schema

```bash
psql -d healthcare_oltp -f 00_oltp_schema.sql
```

Confirm the tables exist:

```bash
psql -d healthcare_oltp -c "\dt"
```

---

### 3) Generate the 10k dataset

This creates ~10,000 rows per table (balanced dates so monthly queries aren’t all in one month).

```bash
psql -d healthcare_oltp -f 03_generate_10k_balanced.sql
```

Optional sanity check (encounters per month):

```bash
psql -d healthcare_oltp -c "
SELECT date_trunc('month', encounter_date)::date AS month_start, COUNT(*)
FROM encounters
GROUP BY 1
ORDER BY 1
LIMIT 12;
"
```

---

### 4) Run the baseline (OLTP) analytics queries

```bash
psql -d healthcare_oltp -f 11_baseline_queries_v2.sql
```

Where results are saved:

- `perf/baseline_explain_v2/` (EXPLAIN ANALYZE output for Q1–Q4)

---

### 5) Create the star schema (DW tables)

```bash
psql -d healthcare_oltp -f star_schema.sql
```

Confirm DW tables exist:

```bash
psql -d healthcare_oltp -c "\dt dw.*"
```

---

### 6) Run the ETL (load the star schema)

```bash
psql -d healthcare_oltp -f 30_load_dw.sql
```

Quick checks:

```bash
psql -d healthcare_oltp -c "SELECT COUNT(*) AS fact_rows FROM dw.fact_encounters;"
psql -d healthcare_oltp -c "SELECT COUNT(*) AS diag_bridge_rows FROM dw.bridge_encounter_diagnoses;"
psql -d healthcare_oltp -c "SELECT COUNT(*) AS proc_bridge_rows FROM dw.bridge_encounter_procedures;"
```

---

### 7) Run the star schema analytics queries

```bash
psql -d healthcare_oltp -f 40_star_queries.sql
```

Where results are saved:

- `perf/star_explain/` (EXPLAIN ANALYZE output for Q1–Q4)

---

## Where to look for the write-ups (deliverables)

- `query_analysis.txt` — baseline SQL + performance notes + bottlenecks
- `design_decisions.txt` — why the star schema is designed this way
- `star_schema_queries.txt` — star SQL + performance comparison vs baseline
- `etl_design.txt` — how the ETL works (dimensions, fact, bridges)
- `reflection.md` — what improved, what trade-offs we made, and the numbers

````
