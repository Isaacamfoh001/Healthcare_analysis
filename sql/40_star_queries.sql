-- sql/40_star_queries.sql
-- Star schema versions of Q1-Q4 (DW schema = dw)

-- =========================================================
-- Q1 (STAR): Monthly encounters by specialty + encounter type
-- =========================================================
SELECT
  d.year,
  d.month,
  (DATE_TRUNC('month', d.calendar_date))::date AS month_start,
  s.specialty_name,
  et.type_name AS encounter_type,
  COUNT(*) AS total_encounters,
  COUNT(DISTINCT f.patient_key) AS unique_patients
FROM dw.fact_encounters f
JOIN dw.dim_date d ON f.encounter_date_key = d.date_key
JOIN dw.dim_specialty s ON f.specialty_key = s.specialty_key
JOIN dw.dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY 1,2,3,4,5
ORDER BY month_start, specialty_name, encounter_type;

-- =========================================================
-- Q2 (STAR): Top diagnosis-procedure pairs
-- =========================================================
SELECT
  dg.icd10_code,
  pr.cpt_code,
  COUNT(*) AS pair_rows,
  COUNT(DISTINCT f.encounter_key) AS encounter_count
FROM dw.fact_encounters f
JOIN dw.bridge_encounter_diagnoses bed ON f.encounter_key = bed.encounter_key
JOIN dw.dim_diagnosis dg ON bed.diagnosis_key = dg.diagnosis_key
JOIN dw.bridge_encounter_procedures bep ON f.encounter_key = bep.encounter_key
JOIN dw.dim_procedure pr ON bep.procedure_key = pr.procedure_key
GROUP BY 1,2
ORDER BY encounter_count DESC, pair_rows DESC
LIMIT 20;

-- =========================================================
-- Q3 (STAR): 30-day readmission rate by specialty
-- Use fact_encounters for encounter/discharge dates and patient_key
-- =========================================================
WITH inpatient_discharges AS (
  SELECT
    f.encounter_key,
    f.patient_key,
    f.specialty_key,
    dd.calendar_date AS discharge_date
  FROM dw.fact_encounters f
  JOIN dw.dim_date dd ON f.discharge_date_key = dd.date_key
  WHERE f.is_inpatient_flag = TRUE
    AND f.discharge_date_key IS NOT NULL
),
flagged AS (
  SELECT
    i.specialty_key,
    CASE WHEN EXISTS (
      SELECT 1
      FROM dw.fact_encounters f2
      JOIN dw.dim_date d2 ON f2.encounter_date_key = d2.date_key
      WHERE f2.patient_key = i.patient_key
        AND d2.calendar_date > i.discharge_date
        AND d2.calendar_date <= i.discharge_date + INTERVAL '30 days'
    ) THEN 1 ELSE 0 END AS is_readmitted_30d
  FROM inpatient_discharges i
)
SELECT
  s.specialty_name,
  COUNT(*) AS inpatient_discharges,
  SUM(is_readmitted_30d) AS readmissions_30d,
  ROUND((SUM(is_readmitted_30d)::numeric / NULLIF(COUNT(*), 0)) * 100, 2) AS readmission_rate_pct
FROM flagged f
JOIN dw.dim_specialty s ON f.specialty_key = s.specialty_key
GROUP BY 1
ORDER BY readmission_rate_pct DESC, inpatient_discharges DESC;

-- =========================================================
-- Q4 (STAR): Revenue by specialty & month
-- (uses pre-aggregated total_allowed_amount in fact)
-- =========================================================
SELECT
  (DATE_TRUNC('month', d.calendar_date))::date AS month_start,
  s.specialty_name,
  SUM(f.total_allowed_amount) AS total_allowed_amount,
  SUM(f.claim_count) AS total_claims
FROM dw.fact_encounters f
JOIN dw.dim_date d ON f.encounter_date_key = d.date_key
JOIN dw.dim_specialty s ON f.specialty_key = s.specialty_key
GROUP BY 1,2
ORDER BY month_start, total_allowed_amount DESC;
