{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_medical_insurance') }}
)

select
    -- Foreign Keys (link to Dimensions)
    patient_id,
    
    -- Insurance Details
    insure_plan_type,
    policy_term_years,
    
    -- Financial Metrics
    monthly_premium,
    annual_premium,
    annual_medical_cost as claim_amount,
    
    -- Loss Ratio Calculation
    -- Logic: prevent division by zero using nullif and multiply by 100 to express as percentage
    round(
        ((annual_medical_cost / nullif(annual_premium, 0)) * 100)::numeric, 
        2
    ) as loss_ratio_percentage,
    
    -- Profit/Loss
    (annual_premium - annual_medical_cost) as underwriting_profit,

    -- Risk Analysis: ตรวจจับเคสที่ขาดทุน (Loss Ratio > 100%)
    case 
        when annual_medical_cost > annual_premium then true 
        else false 
    end as is_unprofitable,

    -- Future Proofing: Medical Inflation Adjustment (assume 5% per year)
    round((annual_medical_cost * 1.05)::numeric, 2) as projected_cost_next_year_inflation

    -- TODO: surcharge revenue (fee income) if available in future datasets

from staging
