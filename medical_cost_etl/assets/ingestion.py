import pandas as pd
import os
from dagster import asset, TableSchema, TableColumn
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# DB connection setting 
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT")
DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@asset(
    key_prefix=["raw"],
    group_name="ingestion_layer",
    compute_kind="pandas",
    metadata={
        "schema": TableSchema(
            columns=[
                TableColumn(name="person_id", type="bigint"),
                TableColumn(name="age", type="bigint"),
                TableColumn(name="sex", type="text"),
                TableColumn(name="region", type="text"),
                TableColumn(name="urban_rural", type="text"),
                TableColumn(name="income", type="double precision"),
                TableColumn(name="education", type="text"),
                TableColumn(name="marital_status", type="text"),
                TableColumn(name="employment_status", type="text"),
                TableColumn(name="household_size", type="bigint"),
                TableColumn(name="dependents", type="bigint"),
                TableColumn(name="bmi", type="double precision"),
                TableColumn(name="smoker", type="text"),
                TableColumn(name="alcohol_freq", type="text"),
                TableColumn(name="visits_last_year", type="bigint"),
                TableColumn(name="hospitalizations_last_3yrs", type="bigint"),
                TableColumn(name="days_hospitalized_last_3yrs", type="bigint"),
                TableColumn(name="medication_count", type="bigint"),
                TableColumn(name="systolic_bp", type="double precision"),
                TableColumn(name="diastolic_bp", type="double precision"),
                TableColumn(name="ldl", type="double precision"),
                TableColumn(name="hba1c", type="double precision"),
                TableColumn(name="plan_type", type="text"),
                TableColumn(name="network_tier", type="text"),
                TableColumn(name="deductible", type="bigint"),
                TableColumn(name="copay", type="bigint"),
                TableColumn(name="policy_term_years", type="bigint"),
                TableColumn(name="policy_changes_last_2yrs", type="bigint"),
                TableColumn(name="provider_quality", type="double precision"),
                TableColumn(name="risk_score", type="double precision"),
                TableColumn(name="annual_medical_cost", type="double precision"),
                TableColumn(name="annual_premium", type="double precision"),
                TableColumn(name="monthly_premium", type="double precision"),
                TableColumn(name="claims_count", type="bigint"),
                TableColumn(name="avg_claim_amount", type="double precision"),
                TableColumn(name="total_claims_paid", type="double precision"),
                TableColumn(name="chronic_count", type="bigint"),
                TableColumn(name="hypertension", type="bigint"),
                TableColumn(name="diabetes", type="bigint"),
                TableColumn(name="asthma", type="bigint"),
                TableColumn(name="copd", type="bigint"),
                TableColumn(name="cardiovascular_disease", type="bigint"),
                TableColumn(name="cancer_history", type="bigint"),
                TableColumn(name="kidney_disease", type="bigint"),
                TableColumn(name="liver_disease", type="bigint"),
                TableColumn(name="arthritis", type="bigint"),
                TableColumn(name="mental_health", type="bigint"),
                TableColumn(name="proc_imaging_count", type="bigint"),
                TableColumn(name="proc_surgery_count", type="bigint"),
                TableColumn(name="proc_physio_count", type="bigint"),
                TableColumn(name="proc_consult_count", type="bigint"),
                TableColumn(name="proc_lab_count", type="bigint"),
                TableColumn(name="is_high_risk", type="bigint"),
                TableColumn(name="had_major_procedure", type="bigint")
            ]
        )
    }
)
def raw_medical_insurance(context):
    """
    read CSV file and ingest into PostgreSQL (raw_medical_insurance table)
    """
    csv_path = "data/medical_insurance.csv"
    df = pd.read_csv(csv_path)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    engine = create_engine(DB_CONNECTION_STRING)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE public.raw_medical_insurance"))
        conn.commit()
        
    df.to_sql(
        name="raw_medical_insurance",
        con=engine,
        schema="public",
        if_exists="append",
        index=False,
        chunksize=1000
    )

    return df.head()
