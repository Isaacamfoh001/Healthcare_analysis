-- sql/20_star_schema.sql
-- Star schema for healthcare analytics (PostgreSQL)
-- Uses schema "dw" to separate OLTP from analytics tables.

BEGIN;

-- -------------------------------------------------------------------
-- Create a dedicated schema for the dimensional model
-- -------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dw;

-- -------------------------------------------------------------------
-- Drop existing DW objects (safe reruns)
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS dw.bridge_encounter_procedures;
DROP TABLE IF EXISTS dw.bridge_encounter_diagnoses;
DROP TABLE IF EXISTS dw.fact_encounters;

DROP TABLE IF EXISTS dw.dim_procedure;
DROP TABLE IF EXISTS dw.dim_diagnosis;
DROP TABLE IF EXISTS dw.dim_encounter_type;
DROP TABLE IF EXISTS dw.dim_department;
DROP TABLE IF EXISTS dw.dim_specialty;
DROP TABLE IF EXISTS dw.dim_provider;
DROP TABLE IF EXISTS dw.dim_patient;
DROP TABLE IF EXISTS dw.dim_date;

-- -------------------------------------------------------------------
-- Dimensions
-- -------------------------------------------------------------------

-- Date dimension (role-playing: encounter_date_key, discharge_date_key, claim_date_key, procedure_date_key)
CREATE TABLE dw.dim_date (
  date_key        INT PRIMARY KEY,            -- e.g. 20240131
  calendar_date   DATE NOT NULL UNIQUE,
  year            SMALLINT NOT NULL,
  quarter         SMALLINT NOT NULL,
  month           SMALLINT NOT NULL,
  month_name      VARCHAR(12) NOT NULL,
  day_of_month    SMALLINT NOT NULL,
  day_of_week     SMALLINT NOT NULL,          -- 1=Mon..7=Sun (we'll define in ETL)
  week_of_year    SMALLINT NOT NULL,
  is_weekend      BOOLEAN NOT NULL
);
COMMENT ON TABLE dw.dim_date IS 'Calendar date dimension used for encounter, discharge, claim, and procedure dates.';

-- Patient dimension
CREATE TABLE dw.dim_patient (
  patient_key     BIGSERIAL PRIMARY KEY,
  patient_id      INT NOT NULL UNIQUE,         -- natural key from OLTP
  mrn             VARCHAR(20) UNIQUE,
  first_name      VARCHAR(100),
  last_name       VARCHAR(100),
  gender          CHAR(1),
  date_of_birth   DATE,
  age_group       VARCHAR(20)                  -- derived during load
);
COMMENT ON TABLE dw.dim_patient IS 'Patient dimension with demographics and derived age_group.';

-- Specialty dimension
CREATE TABLE dw.dim_specialty (
  specialty_key   BIGSERIAL PRIMARY KEY,
  specialty_id    INT NOT NULL UNIQUE,         -- natural key
  specialty_name  VARCHAR(100) NOT NULL,
  specialty_code  VARCHAR(10)
);
COMMENT ON TABLE dw.dim_specialty IS 'Specialty dimension (e.g., Cardiology).';

-- Department dimension
CREATE TABLE dw.dim_department (
  department_key   BIGSERIAL PRIMARY KEY,
  department_id    INT NOT NULL UNIQUE,        -- natural key
  department_name  VARCHAR(100) NOT NULL,
  floor            INT,
  capacity         INT
);
COMMENT ON TABLE dw.dim_department IS 'Hospital department dimension.';

-- Provider dimension
CREATE TABLE dw.dim_provider (
  provider_key     BIGSERIAL PRIMARY KEY,
  provider_id      INT NOT NULL UNIQUE,        -- natural key
  first_name       VARCHAR(100),
  last_name        VARCHAR(100),
  credential       VARCHAR(20),
  specialty_id     INT,                        -- lineage (optional)
  department_id    INT                         -- lineage (optional)
);
COMMENT ON TABLE dw.dim_provider IS 'Provider dimension; specialty/department IDs kept for lineage.';

-- Encounter type dimension
CREATE TABLE dw.dim_encounter_type (
  encounter_type_key  SMALLSERIAL PRIMARY KEY,
  type_name           VARCHAR(50) NOT NULL UNIQUE
);
COMMENT ON TABLE dw.dim_encounter_type IS 'Encounter type dimension (Outpatient/Inpatient/ER).';

-- Diagnosis dimension
CREATE TABLE dw.dim_diagnosis (
  diagnosis_key    BIGSERIAL PRIMARY KEY,
  diagnosis_id     INT NOT NULL UNIQUE,        -- natural key
  icd10_code       VARCHAR(10) NOT NULL,
  icd10_description VARCHAR(200)
);
COMMENT ON TABLE dw.dim_diagnosis IS 'Diagnosis dimension (ICD-10).';

-- Procedure dimension
CREATE TABLE dw.dim_procedure (
  procedure_key    BIGSERIAL PRIMARY KEY,
  procedure_id     INT NOT NULL UNIQUE,        -- natural key
  cpt_code         VARCHAR(10) NOT NULL,
  cpt_description  VARCHAR(200)
);
COMMENT ON TABLE dw.dim_procedure IS 'Procedure dimension (CPT).';

-- -------------------------------------------------------------------
-- Fact table (grain: 1 row per encounter)
-- -------------------------------------------------------------------
CREATE TABLE dw.fact_encounters (
  encounter_key          BIGSERIAL PRIMARY KEY,
  encounter_id           INT NOT NULL UNIQUE,        -- natural key (OLTP)

  -- Dimension foreign keys
  patient_key            BIGINT NOT NULL REFERENCES dw.dim_patient(patient_key),
  provider_key           BIGINT NOT NULL REFERENCES dw.dim_provider(provider_key),
  specialty_key          BIGINT NOT NULL REFERENCES dw.dim_specialty(specialty_key),
  department_key         BIGINT NOT NULL REFERENCES dw.dim_department(department_key),
  encounter_type_key     SMALLINT NOT NULL REFERENCES dw.dim_encounter_type(encounter_type_key),

  encounter_date_key     INT NOT NULL REFERENCES dw.dim_date(date_key),
  discharge_date_key     INT REFERENCES dw.dim_date(date_key),  -- nullable

  -- Pre-aggregated / derived metrics
  diagnosis_count        INT NOT NULL DEFAULT 0,
  procedure_count        INT NOT NULL DEFAULT 0,
  claim_count            INT NOT NULL DEFAULT 0,
  total_claim_amount     NUMERIC(12,2) NOT NULL DEFAULT 0,
  total_allowed_amount   NUMERIC(12,2) NOT NULL DEFAULT 0,
  length_of_stay_days    INT,                                  -- derived
  is_inpatient_flag      BOOLEAN NOT NULL DEFAULT FALSE
);
COMMENT ON TABLE dw.fact_encounters IS 'Encounter fact table (1 row per encounter) with pre-aggregated metrics.';

-- Helpful indexes for analytic queries
CREATE INDEX idx_fact_encounter_date_key ON dw.fact_encounters(encounter_date_key);
CREATE INDEX idx_fact_specialty_date ON dw.fact_encounters(specialty_key, encounter_date_key);
CREATE INDEX idx_fact_patient_encdate ON dw.fact_encounters(patient_key, encounter_date_key);
CREATE INDEX idx_fact_inpatient ON dw.fact_encounters(is_inpatient_flag) WHERE is_inpatient_flag = TRUE;

-- -------------------------------------------------------------------
-- Bridge tables for many-to-many
-- -------------------------------------------------------------------

CREATE TABLE dw.bridge_encounter_diagnoses (
  encounter_key     BIGINT NOT NULL REFERENCES dw.fact_encounters(encounter_key) ON DELETE CASCADE,
  diagnosis_key     BIGINT NOT NULL REFERENCES dw.dim_diagnosis(diagnosis_key),
  diagnosis_sequence INT,
  PRIMARY KEY (encounter_key, diagnosis_key)
);
COMMENT ON TABLE dw.bridge_encounter_diagnoses IS 'Bridge: encounters to diagnoses (many-to-many).';

CREATE INDEX idx_bed_diagnosis_key ON dw.bridge_encounter_diagnoses(diagnosis_key);

CREATE TABLE dw.bridge_encounter_procedures (
  encounter_key     BIGINT NOT NULL REFERENCES dw.fact_encounters(encounter_key) ON DELETE CASCADE,
  procedure_key     BIGINT NOT NULL REFERENCES dw.dim_procedure(procedure_key),
  procedure_date_key INT REFERENCES dw.dim_date(date_key),
  PRIMARY KEY (encounter_key, procedure_key)
);
COMMENT ON TABLE dw.bridge_encounter_procedures IS 'Bridge: encounters to procedures (many-to-many).';

CREATE INDEX idx_bep_procedure_key ON dw.bridge_encounter_procedures(procedure_key);
CREATE INDEX idx_bep_proc_date_key ON dw.bridge_encounter_procedures(procedure_date_key);

COMMIT;
