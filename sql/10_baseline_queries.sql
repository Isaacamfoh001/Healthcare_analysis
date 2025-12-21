-- sql/10_baseline_queries.sql
-- Baseline OLTP queries (normalized schema)
-- Day 2: Q1 + Q4

-- =========================================================
-- Q1: Monthly Encounters by Specialty
-- For each month and specialty, show total encounters
-- and unique patients by encounter type.
-- =========================================================
SELECT
  date_trunc('month', e.encounter_date)::date AS month_start,
  s.specialty_name,
  e.encounter_type,
  COUNT(*) AS total_encounters,
  COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p
  ON e.provider_id = p.provider_id
JOIN specialties s
  ON p.specialty_id = s.specialty_id
GROUP BY
  date_trunc('month', e.encounter_date)::date,
  s.specialty_name,
  e.encounter_type
ORDER BY
  month_start,
  s.specialty_name,
  e.encounter_type;

-- =========================================================
-- Q4: Revenue by Specialty & Month
-- Total allowed amounts by specialty and month.
-- Join chain: billing -> encounters -> providers -> specialties
-- =========================================================
SELECT
  date_trunc('month', b.claim_date)::date AS month_start,
  s.specialty_name,
  SUM(b.allowed_amount) AS total_allowed_amount,
  COUNT(*) AS total_claims
FROM billing b
JOIN encounters e
  ON b.encounter_id = e.encounter_id
JOIN providers p
  ON e.provider_id = p.provider_id
JOIN specialties s
  ON p.specialty_id = s.specialty_id
GROUP BY
  date_trunc('month', b.claim_date)::date,
  s.specialty_name
ORDER BY
  month_start,
  total_allowed_amount DESC;
