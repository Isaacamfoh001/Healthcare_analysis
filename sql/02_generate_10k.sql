-- sql/02_generate_10k.sql
-- PostgreSQL synthetic data generator: 10,000 rows per OLTP table

BEGIN;

TRUNCATE TABLE
  billing,
  encounter_procedures,
  encounter_diagnoses,
  encounters,
  providers,
  patients,
  procedures,
  diagnoses,
  departments,
  specialties
RESTART IDENTITY CASCADE;

-- SELECT setseed(0.42); -- optional deterministic randomness

INSERT INTO specialties (specialty_id, specialty_name, specialty_code)
SELECT i, 'Specialty ' || i, 'SP' || lpad(i::text, 5, '0')
FROM generate_series(1, 10000) AS i;

INSERT INTO departments (department_id, department_name, floor, capacity)
SELECT
  i,
  'Department ' || i,
  1 + floor(random() * 10)::int,
  10 + floor(random() * 91)::int
FROM generate_series(1, 10000) AS i;

INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, gender, mrn)
SELECT
  i,
  (ARRAY['John','Jane','Robert','Mary','Michael','Sarah','David','Linda','Daniel','Emily',
         'Chris','Laura','James','Sophia','Brian','Olivia','Kevin','Grace','Samuel','Nora'])
    [1 + floor(random()*20)::int],
  (ARRAY['Smith','Johnson','Williams','Brown','Jones','Miller','Davis','Garcia','Rodriguez','Wilson',
         'Martinez','Anderson','Taylor','Thomas','Hernandez','Moore','Martin','Jackson','Thompson','White'])
    [1 + floor(random()*20)::int],
  (DATE '1940-01-01' + floor(random() * (DATE '2010-12-31' - DATE '1940-01-01'))::int),
  CASE WHEN random() < 0.49 THEN 'M' ELSE 'F' END,
  'MRN' || lpad(i::text, 6, '0')
FROM generate_series(1, 10000) AS i;

INSERT INTO diagnoses (diagnosis_id, icd10_code, icd10_description)
SELECT i, 'D' || lpad(i::text, 5, '0'), 'Diagnosis description ' || i
FROM generate_series(1, 10000) AS i;

INSERT INTO procedures (procedure_id, cpt_code, cpt_description)
SELECT i, 'P' || lpad(i::text, 5, '0'), 'Procedure description ' || i
FROM generate_series(1, 10000) AS i;

INSERT INTO providers (provider_id, first_name, last_name, credential, specialty_id, department_id)
SELECT
  i,
  (ARRAY['James','Sarah','Michael','Emily','Daniel','Grace','Henry','Ava','Noah','Mia',
         'Lucas','Zoe','Ethan','Lily','Ryan','Ella','Jack','Chloe','Leo','Hannah'])
    [1 + floor(random()*20)::int],
  (ARRAY['Chen','Williams','Rodriguez','Patel','Nguyen','Kim','Singh','Hassan','Mensah','Boateng',
         'Owusu','Asare','Amoah','Boadu','Addo','Quartey','Ali','Ibrahim','Okafor','Adeyemi'])
    [1 + floor(random()*20)::int],
  (ARRAY['MD','DO','NP','PA'])[1 + floor(random()*4)::int],
  1 + floor(random() * 10000)::int,
  1 + floor(random() * 10000)::int
FROM generate_series(1, 10000) AS i;

INSERT INTO encounters (
  encounter_id, patient_id, provider_id, encounter_type,
  encounter_date, discharge_date, department_id
)
SELECT
  i,
  1 + floor(random() * 10000)::int,
  p.provider_id,
  et.encounter_type,
  et.encounter_date,
  et.discharge_date,
  p.department_id
FROM generate_series(1, 10000) AS i
JOIN LATERAL (
  SELECT provider_id, department_id
  FROM providers
  WHERE provider_id = (1 + floor(random() * 10000)::int)
) p ON TRUE
JOIN LATERAL (
  SELECT
    CASE
      WHEN random() < 0.60 THEN 'Outpatient'
      WHEN random() < 0.85 THEN 'Inpatient'
      ELSE 'ER'
    END AS encounter_type,
    (
      TIMESTAMP '2023-01-01'
      + (random() * (TIMESTAMP '2024-12-31' - TIMESTAMP '2023-01-01'))
    ) AS encounter_date
) base ON TRUE
JOIN LATERAL (
  SELECT
    base.encounter_type,
    base.encounter_date,
    CASE
      WHEN base.encounter_type = 'Outpatient'
        THEN base.encounter_date + (30 + floor(random()*151)::int) * INTERVAL '1 minute'
      WHEN base.encounter_type = 'ER'
        THEN base.encounter_date + (1 + floor(random()*10)::int) * INTERVAL '1 hour'
      ELSE
        base.encounter_date + (1 + floor(random()*10)::int) * INTERVAL '1 day'
    END AS discharge_date
) et ON TRUE;

INSERT INTO encounter_diagnoses (encounter_diagnosis_id, encounter_id, diagnosis_id, diagnosis_sequence)
SELECT i, i, 1 + floor(random() * 10000)::int, 1
FROM generate_series(1, 10000) AS i;

INSERT INTO encounter_procedures (encounter_procedure_id, encounter_id, procedure_id, procedure_date)
SELECT
  i,
  i,
  1 + floor(random() * 10000)::int,
  (e.encounter_date::date + floor(random()*3)::int)
FROM generate_series(1, 10000) AS i
JOIN encounters e ON e.encounter_id = i;

-- 10) BILLING (10,000)
-- Important: ensure round() receives NUMERIC, not DOUBLE PRECISION
INSERT INTO billing (billing_id, encounter_id, claim_amount, allowed_amount, claim_date, claim_status)
SELECT
  i,
  i AS encounter_id,
  round(((50 + random()*20000)::numeric), 2) AS claim_amount,
  round((((50 + random()*20000)::numeric) * (0.70 + random()*0.30)::numeric), 2) AS allowed_amount,
  (e.encounter_date::date + floor(random()*30)::int) AS claim_date,
  (ARRAY['Paid','Denied','Pending','Adjusted'])[1 + floor(random()*4)::int] AS claim_status
FROM generate_series(1, 10000) AS i
JOIN encounters e ON e.encounter_id = i;
ANALYZE;
COMMIT;

