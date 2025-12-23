# Reflection: OLTP vs Star Schema (Healthcare Analytics)

## Why the Star Schema Is Faster

The OLTP database is designed for correctness and transactional updates. It’s normalized, so attributes like specialty, provider, billing, diagnoses, and procedures live in separate tables and are stitched together during queries.

That structure is fine for CRUD operations, but it becomes painful for analytics. The baseline queries had a few recurring patterns:

- Long JOIN chains just to get to the attributes needed for grouping (e.g., encounters → providers → specialties).
- Expensive GROUP BY + ORDER BY operations after joining multiple tables.
- For readmissions, repeated lookups back into the encounters table to find a follow-up visit within 30 days.
- Diagnosis/procedure analysis needing two junction tables, which is the classic “row explosion” risk as data grows.

In the star schema, the fact table sits in the middle, and the most-used reporting attributes are one join away. Some measures are also pre-computed during the ETL so we don’t have to re-join billing or re-count related rows every time.

### Join count comparison (practical view)
- **Q4 baseline (OLTP)**: billing → encounters → providers → specialties (3 joins before you even aggregate).
- **Q4 star (DW)**: fact_encounters → dim_date → dim_specialty (2 joins, and no billing join at query time).

- **Q3 baseline (OLTP)**: inpatient encounters + an `EXISTS` lookup back into encounters, executed many times.
- **Q3 star (DW)**: still uses an `EXISTS` pattern, but it runs against a tighter structure (fact_encounters) and benefits from DW indexing and simpler key comparisons.

So the real win is not “no joins”, it’s:
- fewer joins on the hot paths (Q1/Q4),
- and more predictable, index-friendly access patterns for harder questions (Q3).


## Where the Star Schema Pre-computes Work

The ETL loads encounter-level metrics into `dw.fact_encounters`, including:
- `total_allowed_amount` and `claim_count` (billing rolled up to encounter level)
- `diagnosis_count` and `procedure_count`
- `is_inpatient_flag` and `length_of_stay_days`

This is the main reason Q4 becomes cheaper: the revenue number is already attached to the encounter row, so we avoid joining and aggregating billing repeatedly.


## Trade-offs: What We Gained vs What We Lost

### Gains
- Queries are simpler to write and reason about.
- Joins are more predictable (fact → dimension).
- We can add indexes specifically for analytics patterns (like inpatient-only filtering, patient/date lookups).
- Reporting workloads are faster and don’t punish the OLTP schema.

### Costs
- We duplicated data. Specialty and provider names exist in both OLTP and DW now.
- We added ETL complexity. Instead of “just query the OLTP”, we have to load and maintain the DW.
- Some analytics (like diagnosis/procedure combinations) still require bridge joins, so those queries aren’t “free”.

Overall, for analytics-heavy use cases, the trade-off is worth it. OLTP stays clean for transactions, and DW is optimized for reading and aggregation.


## Bridge Tables: Worth It?

Yes, for this project it’s the right choice.

Encounters have many diagnoses and many procedures. If we tried to store these directly inside the fact table, we would either:
- change the grain (one row per diagnosis or procedure), which would duplicate encounter-level measures like revenue and inflate encounter counts, or
- store arrays / repeated columns, which is awkward for SQL analytics.

Bridge tables keep the encounter grain stable while still allowing detailed analysis when needed (like diagnosis-procedure pairs in Q2).

The trade-off is that Q2 still needs multiple joins, but that’s acceptable because it’s a specialised query, while the day-to-day reporting (Q1/Q3/Q4) benefits from a clean fact table.


## Performance Quantification (Baseline v2 vs Star)

Here are the measured times from EXPLAIN (ANALYZE, BUFFERS):

### Query 3: 30-Day Readmission Rate
- Baseline (OLTP): **281.255 ms**
- Star (DW): **68.777 ms**
- Improvement: **281.255 / 68.777 = 4.09x faster**

Main reason:
- The DW plan is more index-friendly and runs on a smaller, analytics-shaped structure.
- Filtering inpatient rows is fast (partial index on inpatient flag), and patient/date access is more direct.

### Query 4: Revenue by Specialty & Month
- Baseline (OLTP): **44.011 ms**
- Star (DW): **37.517 ms**
- Improvement: **44.011 / 37.517 = 1.17x faster**

Main reason:
- Revenue is already pre-aggregated at the encounter level (`total_allowed_amount`), so the query avoids joining billing at runtime.
- The remaining cost is mostly GROUP BY and sorting across many specialty groups.

(Other queries had smaller improvements because they still require heavy grouping/sorting, and in this lab the dimensions are unusually large due to the “10k per table” requirement. In a more realistic warehouse, dimensions are much smaller than the fact table, which usually makes the star schema benefits even more obvious.)


## Was it worth it?

For analytics, yes.

The OLTP schema is still useful as the source of truth for operational workflows. But the moment the business starts asking “monthly trends”, “top combinations”, and “readmission rates”, the star schema approach gives a cleaner query experience and stronger performance, especially as data scales beyond the lab’s 10k rows.
