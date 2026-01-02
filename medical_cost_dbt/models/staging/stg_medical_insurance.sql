
with source_data as (
    select *
    from {{source('raw', 'raw_medical_insurance')}}
    where person_id is not null
),

renamed_data as (
    select
        person_id as patient_id,
        age as patient_age,
        sex,
        region,
        urban_rural,
        income,
        education,
        marital_status,
        employment_status,
        household_size,
        dependents as num_dependents,
        bmi,
        smoker as smoking_status,
        alcohol_freq,
        visits_last_year as doctor_visits_last_year,
        hospitalizations_last_3yrs,
        days_hospitalized_last_3yrs,
        medication_count,
        systolic_bp,
        diastolic_bp,
        ldl,
        hba1c,
        plan_type as insure_plan_type,
        network_tier as insure_network_tier,
        deductible as deductible_amount,
        copay,
        policy_term_years,
        policy_changes_last_2yrs,
        provider_quality as quality_rating,
        risk_score,
        annual_medical_cost,
        annual_premium,
        monthly_premium,
        claims_count,
        avg_claim_amount,
        total_claims_paid,
        chronic_count,
        hypertension as is_hypertension,
        diabetes as is_diabetes,
        asthma as is_asthma,
        copd as is_copd,
        cardiovascular_disease as is_cardiovascular_disease,
        cancer_history as is_cancer_history,
        kidney_disease as is_kidney_disease,
        liver_disease as is_liver_disease,
        arthritis as is_arthritis,
        mental_health as is_mental_issue,
        proc_imaging_count,
        proc_surgery_count as surgery_count,
        proc_physio_count as physiotherapy_count,
        proc_consult_count as specialist_consult_count,
        proc_lab_count as lab_test_count,
        is_high_risk,
        had_major_procedure
    from source_data
)

select * from renamed_data
