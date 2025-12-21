-- sql/00_oltp_schema.sql
-- OLTP schema (3NF) for Healthcare Analytics Lab - PostgreSQL compatible

BEGIN;

DROP TABLE IF EXISTS billing CASCADE;
DROP TABLE IF EXISTS encounter_procedures CASCADE;
DROP TABLE IF EXISTS encounter_diagnoses CASCADE;
DROP TABLE IF EXISTS encounters CASCADE;
DROP TABLE IF EXISTS providers CASCADE;
DROP TABLE IF EXISTS patients CASCADE;
DROP TABLE IF EXISTS procedures CASCADE;
DROP TABLE IF EXISTS diagnoses CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS specialties CASCADE;

CREATE TABLE patients (
  patient_id INT PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  date_of_birth DATE,
  gender CHAR(1),
  mrn VARCHAR(20) UNIQUE
);

CREATE TABLE specialties (
  specialty_id INT PRIMARY KEY,
  specialty_name VARCHAR(100),
  specialty_code VARCHAR(10)
);

CREATE TABLE departments (
  department_id INT PRIMARY KEY,
  department_name VARCHAR(100),
  floor INT,
  capacity INT
);

CREATE TABLE providers (
  provider_id INT PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  credential VARCHAR(20),
  specialty_id INT REFERENCES specialties (specialty_id),
  department_id INT REFERENCES departments (department_id)
);

CREATE TABLE encounters (
  encounter_id INT PRIMARY KEY,
  patient_id INT REFERENCES patients (patient_id),
  provider_id INT REFERENCES providers (provider_id),
  encounter_type VARCHAR(50), -- 'Outpatient', 'Inpatient', 'ER'
  encounter_date TIMESTAMP,
  discharge_date TIMESTAMP,
  department_id INT REFERENCES departments (department_id)
);

CREATE INDEX idx_encounter_date ON encounters(encounter_date);

CREATE TABLE diagnoses (
  diagnosis_id INT PRIMARY KEY,
  icd10_code VARCHAR(10),
  icd10_description VARCHAR(200)
);

CREATE TABLE encounter_diagnoses (
  encounter_diagnosis_id INT PRIMARY KEY,
  encounter_id INT REFERENCES encounters (encounter_id),
  diagnosis_id INT REFERENCES diagnoses (diagnosis_id),
  diagnosis_sequence INT
);

CREATE TABLE procedures (
  procedure_id INT PRIMARY KEY,
  cpt_code VARCHAR(10),
  cpt_description VARCHAR(200)
);

CREATE TABLE encounter_procedures (
  encounter_procedure_id INT PRIMARY KEY,
  encounter_id INT REFERENCES encounters (encounter_id),
  procedure_id INT REFERENCES procedures (procedure_id),
  procedure_date DATE
);

CREATE TABLE billing (
  billing_id INT PRIMARY KEY,
  encounter_id INT REFERENCES encounters (encounter_id),
  claim_amount NUMERIC(12, 2),
  allowed_amount NUMERIC(12, 2),
  claim_date DATE,
  claim_status VARCHAR(50)
);

CREATE INDEX idx_claim_date ON billing(claim_date);

COMMIT;
