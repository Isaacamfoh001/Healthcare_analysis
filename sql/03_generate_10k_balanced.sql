-- sql/03_generate_10k_balanced.sql
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

INSERT INTO specialties (specialty_id, specialty_name, specialty_code)
SELECT i, 'Specialty ' || i, 'SP' || lpad(i::text, 5, '0')
FROM generate_series(1, 10000) AS i;

INSERT INTO departments (department_id, department_name, floor, capacity)
SELECT
  i,
  'Department ' || i,
  1 + ((i - 1) % 10),
  10 + ((i - 1) % 91)
FROM generate_series(1, 10000) AS i;

INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, gender, mrn)
SELECT
  i,
  (ARRAY['John','Jane','Robert','Mary','Michael','Sarah','David','Linda','Daniel','Emily',
         'Chris','Laura','James','Sophia','Brian','Olivia','Kevin','Grace','Samuel','Nora'])
    [1 + ((i - 1) % 20)],
  (ARRAY['Smith','Johnson','Williams','Brown','Jones','Miller','Davis','Garcia','Rodriguez','Wilson',
         'Martinez','Anderson','Taylor','Thomas','Hernandez','Moore','Martin','Jackson','Thompson','White'])
    [1 + ((i - 1) % 20)],
  (DATE '1940-01-01' + ((i - 1) % (DATE '2010-12-31' - DATE '1940-01-01'))),
  CASE WHEN (i % 2) = 0 THEN 'F' ELSE 'M' END,
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
    [1 + ((i - 1) % 20)],
  (ARRAY['Chen','Williams','Rodriguez','Patel','Nguyen','Kim','Singh','Hassan','Mensah','Boateng',
         'Owusu','Asare','Amoah','Boadu','Addo','Quartey','Ali','Ibrahim','Okafor','Adeyemi'])
    [1 + ((i - 1) % 20)],
  (ARRAY['MD','DO','NP','PA'])[1 + ((i - 1) % 4)],
  i,
  i
FROM generate_series(1, 10000) AS i;

-- ✅ IMPORTANT: i comes from generate_series here
INSERT INTO encounters (
  encounter_id, patient_id, provider_id, encounter_type,
  encounter_date, discharge_date, department_id
)
SELECT
  i AS encounter_id,
  1 + ((i - 1) % 10000) AS patient_id,
  1 + ((i - 1) % 10000) AS provider_id,
  CASE
    WHEN (i % 100) < 60 THEN 'Outpatient'
    WHEN (i % 100) < 85 THEN 'Inpatient'
    ELSE 'ER'
  END AS encounter_type,
  (TIMESTAMP '2023-01-01'
    + ((i - 1) % 730) * INTERVAL '1 day'
    + (floor(random()*86400)::int) * INTERVAL '1 second'
  ) AS encounter_date,
  NULL::timestamp AS discharge_date,
  1 + ((i - 1) % 10000) AS department_id
FROM generate_series(1, 10000) AS i;

UPDATE encounters
SET discharge_date =
  CASE
    WHEN encounter_type = 'Outpatient'
      THEN encounter_date + (30 + floor(random()*151)::int) * INTERVAL '1 minute'
    WHEN encounter_type = 'ER'
      THEN encounter_date + (1 + floor(random()*10)::int) * INTERVAL '1 hour'
    ELSE
      encounter_date + (1 + floor(random()*10)::int) * INTERVAL '1 day'
  END;

INSERT INTO encounter_diagnoses (encounter_diagnosis_id, encounter_id, diagnosis_id, diagnosis_sequence)
SELECT
  i,
  i,
  1 + ((i * 37 - 1) % 10000),
  1
FROM generate_series(1, 10000) AS i;

INSERT INTO encounter_procedures (encounter_procedure_id, encounter_id, procedure_id, procedure_date)
SELECT
  i,
  i,
  1 + ((i * 53 - 1) % 10000),
  (e.encounter_date::date + ((i - 1) % 3))
FROM generate_series(1, 10000) AS i
JOIN encounters e ON e.encounter_id = i;

INSERT INTO billing (billing_id, encounter_id, claim_amount, allowed_amount, claim_date, claim_status)
SELECT
  i,
  i,
  round(((50 + random()*20000)::numeric), 2),
  round((((50 + random()*20000)::numeric) * (0.70 + random()*0.30)::numeric), 2),
  (e.encounter_date::date + ((i - 1) % 30)),
  (ARRAY['Paid','Denied','Pending','Adjusted'])[1 + ((i - 1) % 4)]
FROM generate_series(1, 10000) AS i
JOIN encounters e ON e.encounter_id = i;

ANALYZE;

COMMIT;

