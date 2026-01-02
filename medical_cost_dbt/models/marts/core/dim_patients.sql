{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_medical_insurance') }}
)

select
    patient_id,
    patient_age,
    
    -- Age Buckets for Segmentation
    case 
        when patient_age < 20 then '0-19'
        when patient_age between 20 and 40 then '20-40'
        when patient_age between 41 and 60 then '41-60'
        else '60+' 
    end as age_group,
    
    initcap(sex) as sex,
    region,
    urban_rural,
    
    -- Cleaning: Handle Nulls & Standardization for Occupation/Status
    coalesce(employment_status, 'Unemployed') as employment_status,
    coalesce(marital_status, 'Single') as marital_status,
    coalesce(household_size, 1) as household_size,
    income,
    -- Income Classification
    case 
        when income < 40000 then 'Low Income'
        when income between 40000 and 120000 then 'Middle Income'
        else 'High Income'
    end as income_bracket,

    case
        when education like '%HS' then 'High School'
        else education
    end as education,
    num_dependents,

    -- BMI Calculation & Category
    bmi,
    case
        when bmi < 18.5 then 'Underweight'
        when bmi between 18.5 and 24.9 then 'Normal'
        when bmi between 25 and 29.9 then 'Overweight'
        when bmi >= 30 then 'Obese'
        else 'Unknown'
    end as bmi_category,

    smoking_status,
    alcohol_freq,
    case
        when smoking_status = 'Current' and alcohol_freq in ('Daily', 'Weekly') then 'High Risk Lifestyle'
        when smoking_status = 'Never' and alcohol_freq = 'None' then 'Healthy Lifestyle'
        else 'Moderate'
    end as lifestyle_risk_group

from staging
