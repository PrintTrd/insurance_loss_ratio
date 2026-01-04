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
    annual_medical_cost,
    claims_count,
    avg_claim_amount,
    total_claims_paid as annual_claims_paid,
    
    -- Profit/Loss
    (annual_premium - annual_claims_paid) as underwriting_profit,

    -- Risk Analysis: Loss Ratio > 100%
    (underwriting_profit < 0) as is_unprofitable,

    -- Loss Ratio Calculation
    -- Logic: prevent division by zero using nullif and multiply by 100 to express as percentage
    round(
        ((annual_claims_paid / nullif(annual_premium, 0)) * 100)::numeric, 
        2
    ) as loss_ratio_pct,
    
    -- Future Proofing: Medical Inflation Adjustment (assume 5% per year)
    round((annual_claims_paid * 1.05)::numeric, 2) as predicted_cost_inflation_next_year

    -- TODO: surcharge revenue (fee income) if available in future datasets

from staging
