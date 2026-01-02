{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_medical_insurance') }}
)

select
    -- Use person_id as PrimaryKey
    -- In future may use policy_id instead if available
    patient_id,

    -- Plan Attributes
    insure_plan_type,      -- e.g., HMO(lower monthly premiums), PPO(Traditional & Most flexibility), EPO(HMO + PPO), HDHP(lower premiums & higher deductibles)
    insure_network_tier,   -- Basic, Silver, Gold, Platinum

    -- Cost Sharing Structure
    deductible_amount,
    copay,
    
    -- Plan Category
    case 
        when insure_plan_type in ('Bronze', 'Silver') and deductible_amount > 2000 then 'Economy - High Deductible'
        when insure_plan_type in ('Gold', 'Platinum') then 'Premium'
        else 'Standard'
    end as plan_category,

    -- Premium Tier
    case 
        when annual_premium < 1000 then 'Low Premium (<2k)'
        when annual_premium between 1000 and 1800 then 'Medium Premium (2k-8k)'
        else 'High Premium (>8k)'
    end as premium_tier_category,

    -- High Deductible Flag (may lower claim frequency)
    case 
        when deductible_amount >= 2500 then true 
        else false 
    end as is_high_deductible_plan,

    -- Policy Tenure
    policy_term_years,
    policy_changes_last_2yrs,
    
    -- Customer Loyalty Segment
    case
        when policy_term_years < 2 then 'New Customer'
        when policy_term_years between 2 and 5 then 'Loyal'
        else 'Long-term'
    end as loyalty_segment,

    -- Quality Metrics
    quality_rating

from staging
