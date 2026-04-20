/*
DDL for health_care_data_stg and health_care_data_final tables in BigQuery.

CREATE TABLE `project-0bc8a567-62b7-4045-a22.health_care_data.health_care_data_stg` (
    patient_id STRING,
    age STRING,
    gender STRING,
    diagnosis_code STRING,
    diagnosis_description STRING,
    diagnosis_date STRING,
    is_senior STRING,
    validation_status STRING
);,
*/

MERGE INTO `{{ params.project_id }}.{{ params.dataset }}.health_care_data_final` T
USING `{{ params.project_id }}.{{ params.dataset }}.health_care_data_stg` S
ON T.patient_id = S.patient_id AND T.validation_status = S.validation_status
WHEN MATCHED THEN
  UPDATE SET 
    T.age = S.age, 
    T.gender = S.gender, 
    T.diagnosis_description = S.diagnosis_description,
    T.diagnosis_date = S.diagnosis_date,
    T.is_senior = S.is_senior, 
    T.validation_status = S.validation_status
WHEN NOT MATCHED THEN
  INSERT (patient_id, age, gender, diagnosis_code, diagnosis_description, diagnosis_date, is_senior, validation_status)
  VALUES (S.patient_id, S.age, S.gender, S.diagnosis_code, S.diagnosis_description, S.diagnosis_date, S.is_senior, S.validation_status);