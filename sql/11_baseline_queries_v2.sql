-- sql/11_baseline_queries_v2.sql
-- OLTP baseline queries (v2) using balanced 10k dataset

-- =========================================================
-- Q1: Monthly Encounters by Specialty
-- For each month and specialty, show total encounters and unique patients by encounter type.
-- =========================================================
SELECT
  date_trunc('month', e.encounter_date)::date AS month_start,
  s.specialty_name,
  e.encounter_type,
  COUNT(*) AS total_encounters,
  COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- =========================================================
-- Q2: Top Diagnosis-Procedure Pairs
-- Most common diagnosis-procedure combinations.
-- =========================================================
SELECT
  d.icd10_code,
  pr.cpt_code,
  COUNT(*) AS pair_rows,
  COUNT(DISTINCT ed.encounter_id) AS encounter_count
FROM encounter_diagnoses ed
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
JOIN encounter_procedures ep ON ed.encounter_id = ep.encounter_id
JOIN procedures pr ON ep.procedure_id = pr.procedure_id
GROUP BY 1, 2
ORDER BY encounter_count DESC, pair_rows DESC
LIMIT 20;

-- =========================================================
-- Q3: 30-Day Readmission Rate by Specialty
-- Definition: inpatient discharge then return within 30 days
-- =========================================================
WITH index_admissions AS (
  SELECT
    e1.encounter_id,
    e1.patient_id,
    e1.provider_id,
    e1.discharge_date,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM encounters e2
        WHERE e2.patient_id = e1.patient_id
          AND e2.encounter_date > e1.discharge_date
          AND e2.encounter_date <= e1.discharge_date + INTERVAL '30 days'
      ) THEN 1 ELSE 0
    END AS is_readmitted_30d
  FROM encounters e1
  WHERE e1.encounter_type = 'Inpatient'
    AND e1.discharge_date IS NOT NULL
)
SELECT
  s.specialty_name,
  COUNT(*) AS inpatient_discharges,
  SUM(is_readmitted_30d) AS readmissions_30d,
  ROUND((SUM(is_readmitted_30d)::numeric / NULLIF(COUNT(*), 0)) * 100, 2) AS readmission_rate_pct
FROM index_admissions ia
JOIN providers p ON ia.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY 1
ORDER BY readmission_rate_pct DESC, inpatient_discharges DESC;

-- =========================================================
-- Q4: Revenue by Specialty & Month
-- Total allowed amounts by specialty and month.
-- =========================================================
SELECT
  date_trunc('month', b.claim_date)::date AS month_start,
  s.specialty_name,
  SUM(b.allowed_amount) AS total_allowed_amount,
  COUNT(*) AS total_claims
FROM billing b
JOIN encounters e ON b.encounter_id = e.encounter_id
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY 1, 2
ORDER BY month_start, total_allowed_amount DESC;
