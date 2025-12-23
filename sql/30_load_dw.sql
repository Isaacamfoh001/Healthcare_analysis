-- sql/30_load_dw.sql
-- ETL: Load DW star schema from OLTP tables (PostgreSQL)

BEGIN;

-- ------------------------------------------------------------
-- 0) Clear DW tables (reload approach for the lab)
-- ------------------------------------------------------------
TRUNCATE TABLE
  dw.bridge_encounter_procedures,
  dw.bridge_encounter_diagnoses,
  dw.fact_encounters,
  dw.dim_procedure,
  dw.dim_diagnosis,
  dw.dim_encounter_type,
  dw.dim_provider,
  dw.dim_department,
  dw.dim_specialty,
  dw.dim_patient,
  dw.dim_date
RESTART IDENTITY CASCADE;

-- ------------------------------------------------------------
-- 1) dim_date
-- Build date range covering encounter_date, discharge_date, claim_date, procedure_date
-- ------------------------------------------------------------
WITH bounds AS (
  SELECT
    LEAST(
      (SELECT MIN(encounter_date)::date FROM encounters),
      (SELECT MIN(discharge_date)::date FROM encounters WHERE discharge_date IS NOT NULL),
      (SELECT MIN(claim_date) FROM billing),
      (SELECT MIN(procedure_date) FROM encounter_procedures)
    ) AS min_date,
    GREATEST(
      (SELECT MAX(encounter_date)::date FROM encounters),
      (SELECT MAX(discharge_date)::date FROM encounters WHERE discharge_date IS NOT NULL),
      (SELECT MAX(claim_date) FROM billing),
      (SELECT MAX(procedure_date) FROM encounter_procedures)
    ) AS max_date
)
INSERT INTO dw.dim_date (
  date_key, calendar_date, year, quarter, month, month_name,
  day_of_month, day_of_week, week_of_year, is_weekend
)
SELECT
  (EXTRACT(YEAR FROM d)::int * 10000 + EXTRACT(MONTH FROM d)::int * 100 + EXTRACT(DAY FROM d)::int) AS date_key,
  d AS calendar_date,
  EXTRACT(YEAR FROM d)::int AS year,
  EXTRACT(QUARTER FROM d)::int AS quarter,
  EXTRACT(MONTH FROM d)::int AS month,
  TO_CHAR(d, 'Mon') AS month_name,
  EXTRACT(DAY FROM d)::int AS day_of_month,
  EXTRACT(ISODOW FROM d)::int AS day_of_week,
  EXTRACT(WEEK FROM d)::int AS week_of_year,
  (EXTRACT(ISODOW FROM d)::int IN (6,7)) AS is_weekend
FROM bounds,
     generate_series(bounds.min_date, bounds.max_date, interval '1 day') AS d;

-- ------------------------------------------------------------
-- 2) Simple dims from OLTP
-- ------------------------------------------------------------

-- dim_encounter_type
INSERT INTO dw.dim_encounter_type (type_name)
SELECT DISTINCT encounter_type
FROM encounters
ORDER BY 1;

-- dim_specialty
INSERT INTO dw.dim_specialty (specialty_id, specialty_name, specialty_code)
SELECT specialty_id, specialty_name, specialty_code
FROM specialties;

-- dim_department
INSERT INTO dw.dim_department (department_id, department_name, floor, capacity)
SELECT department_id, department_name, floor, capacity
FROM departments;

-- dim_provider
INSERT INTO dw.dim_provider (provider_id, first_name, last_name, credential, specialty_id, department_id)
SELECT provider_id, first_name, last_name, credential, specialty_id, department_id
FROM providers;

-- dim_patient (age_group derived relative to current_date)
INSERT INTO dw.dim_patient (patient_id, mrn, first_name, last_name, gender, date_of_birth, age_group)
SELECT
  p.patient_id,
  p.mrn,
  p.first_name,
  p.last_name,
  p.gender,
  p.date_of_birth,
  CASE
    WHEN p.date_of_birth IS NULL THEN 'Unknown'
    WHEN date_part('year', age(current_date, p.date_of_birth)) < 18 THEN '0-17'
    WHEN date_part('year', age(current_date, p.date_of_birth)) < 35 THEN '18-34'
    WHEN date_part('year', age(current_date, p.date_of_birth)) < 50 THEN '35-49'
    WHEN date_part('year', age(current_date, p.date_of_birth)) < 65 THEN '50-64'
    ELSE '65+'
  END AS age_group
FROM patients p;

-- dim_diagnosis
INSERT INTO dw.dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
SELECT diagnosis_id, icd10_code, icd10_description
FROM diagnoses;

-- dim_procedure
INSERT INTO dw.dim_procedure (procedure_id, cpt_code, cpt_description)
SELECT procedure_id, cpt_code, cpt_description
FROM procedures;

-- ------------------------------------------------------------
-- 3) fact_encounters (1 row per encounter + pre-aggregations)
-- ------------------------------------------------------------

-- Pre-aggregate counts and billing totals per encounter_id
WITH diag_counts AS (
  SELECT encounter_id, COUNT(*) AS diagnosis_count
  FROM encounter_diagnoses
  GROUP BY encounter_id
),
proc_counts AS (
  SELECT encounter_id, COUNT(*) AS procedure_count
  FROM encounter_procedures
  GROUP BY encounter_id
),
bill_aggs AS (
  SELECT
    encounter_id,
    COUNT(*) AS claim_count,
    COALESCE(SUM(claim_amount), 0)::numeric(12,2) AS total_claim_amount,
    COALESCE(SUM(allowed_amount), 0)::numeric(12,2) AS total_allowed_amount
  FROM billing
  GROUP BY encounter_id
)
INSERT INTO dw.fact_encounters (
  encounter_id,
  patient_key, provider_key, specialty_key, department_key, encounter_type_key,
  encounter_date_key, discharge_date_key,
  diagnosis_count, procedure_count,
  claim_count, total_claim_amount, total_allowed_amount,
  length_of_stay_days, is_inpatient_flag
)
SELECT
  e.encounter_id,

  dp.patient_key,
  dprov.provider_key,
  ds.specialty_key,
  dd.department_key,
  det.encounter_type_key,

  denc.date_key AS encounter_date_key,
  ddis.date_key AS discharge_date_key,

  COALESCE(dc.diagnosis_count, 0) AS diagnosis_count,
  COALESCE(pc.procedure_count, 0) AS procedure_count,

  COALESCE(ba.claim_count, 0) AS claim_count,
  COALESCE(ba.total_claim_amount, 0) AS total_claim_amount,
  COALESCE(ba.total_allowed_amount, 0) AS total_allowed_amount,

  CASE
    WHEN e.discharge_date IS NULL THEN NULL
    ELSE GREATEST(0, (e.discharge_date::date - e.encounter_date::date))
  END AS length_of_stay_days,

  (e.encounter_type = 'Inpatient') AS is_inpatient_flag
FROM encounters e
JOIN dw.dim_patient dp
  ON dp.patient_id = e.patient_id
JOIN dw.dim_provider dprov
  ON dprov.provider_id = e.provider_id
JOIN dw.dim_specialty ds
  ON ds.specialty_id = dprov.specialty_id
JOIN dw.dim_department dd
  ON dd.department_id = e.department_id
JOIN dw.dim_encounter_type det
  ON det.type_name = e.encounter_type
JOIN dw.dim_date denc
  ON denc.calendar_date = e.encounter_date::date
LEFT JOIN dw.dim_date ddis
  ON ddis.calendar_date = e.discharge_date::date
LEFT JOIN diag_counts dc
  ON dc.encounter_id = e.encounter_id
LEFT JOIN proc_counts pc
  ON pc.encounter_id = e.encounter_id
LEFT JOIN bill_aggs ba
  ON ba.encounter_id = e.encounter_id;

-- ------------------------------------------------------------
-- 4) Bridge tables (use surrogate keys)
-- ------------------------------------------------------------

-- bridge_encounter_diagnoses
INSERT INTO dw.bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
SELECT
  fe.encounter_key,
  dd.diagnosis_key,
  ed.diagnosis_sequence
FROM encounter_diagnoses ed
JOIN dw.fact_encounters fe
  ON fe.encounter_id = ed.encounter_id
JOIN dw.dim_diagnosis dd
  ON dd.diagnosis_id = ed.diagnosis_id;

-- bridge_encounter_procedures
INSERT INTO dw.bridge_encounter_procedures (encounter_key, procedure_key, procedure_date_key)
SELECT
  fe.encounter_key,
  dp.procedure_key,
  dproc.date_key AS procedure_date_key
FROM encounter_procedures ep
JOIN dw.fact_encounters fe
  ON fe.encounter_id = ep.encounter_id
JOIN dw.dim_procedure dp
  ON dp.procedure_id = ep.procedure_id
LEFT JOIN dw.dim_date dproc
  ON dproc.calendar_date = ep.procedure_date;

-- ------------------------------------------------------------
-- 5) Analyze DW tables for good plans
-- ------------------------------------------------------------
ANALYZE dw.dim_date;
ANALYZE dw.dim_patient;
ANALYZE dw.dim_provider;
ANALYZE dw.dim_specialty;
ANALYZE dw.dim_department;
ANALYZE dw.dim_encounter_type;
ANALYZE dw.dim_diagnosis;
ANALYZE dw.dim_procedure;
ANALYZE dw.fact_encounters;
ANALYZE dw.bridge_encounter_diagnoses;
ANALYZE dw.bridge_encounter_procedures;

COMMIT;
