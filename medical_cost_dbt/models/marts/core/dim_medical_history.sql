{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_medical_insurance') }}
)

select
    patient_id,

    -- Medication Usage
    medication_count,
    case 
        when medication_count >= 5 then 'Polypharmacy (5+ meds)'
        when medication_count between 1 and 4 then 'Moderate Use'
        else 'No Medication'
    end as medication_usage_group,

    -- Health Metrics
    systolic_bp,
    diastolic_bp,
    ldl,
    hba1c,
    
    -- Cleaning: Data Quality Check
    case 
        when systolic_bp > 140 or diastolic_bp > 90 then 'High'
        when systolic_bp < 90 or diastolic_bp < 60 then 'Low'
        else 'Normal'
    end as blood_pressure_status,

    -- Chronic Conditions Flags (Change 1/0 to True/False)
    (is_hypertension = 1) as has_hypertension,
    (is_diabetes = 1) as has_diabetes,
    (is_asthma = 1) as has_asthma,
    (is_copd = 1) as has_copd,
    (is_cardiovascular_disease = 1) as has_cardiovascular_disease,
    (is_cancer_history = 1) as has_cancer_history,
    (is_kidney_disease = 1) as has_kidney_disease,
    (is_liver_disease = 1) as has_liver_disease,
    (is_arthritis = 1) as has_arthritis,
    (is_mental_issue = 1) as has_mental_issue,
    (is_high_risk = 1) as has_high_risk,
    (had_major_procedure = 1) as had_major_procedure,

    chronic_count,
    proc_imaging_count,
    surgery_count,
    physiotherapy_count,
    specialist_consult_count,
    lab_test_count,
    doctor_visits_last_year,
    hospitalizations_last_3yrs,
    days_hospitalized_last_3yrs

from staging
