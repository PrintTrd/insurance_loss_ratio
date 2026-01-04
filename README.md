# ActuaryFlow: Medical Risk & Pricing Engine 🏥📊

**ActuaryFlow** is an end-to-end data engineering pipeline designed for the modern insurance sector. It automates the processing of medical claims data to calculate **Loss Ratios** and prepare high-quality features for **Medical Cost Prediction Models**.

> **"Turning raw claims data into actionable risk insights."**

## 🎯 Business Value & Use Cases

โปรเจกต์นี้ถูกออกแบบมาเพื่อรองรับโจทย์ทางธุรกิจประกันภัยโดยเฉพาะ:

* **📉 Insurance Loss Ratio Monitoring:** คำนวณอัตราส่วนค่าสินไหมทดแทนต่อเบี้ยประกัน (Loss Ratio) แบบอัตโนมัติ เพื่อติดตามสุขภาพทางการเงินของแผนประกันแต่ละประเภท
* **🔮 Pre-Processing for Cost Prediction:** เตรียมข้อมูล (Feature Engineering) ให้พร้อมสำหรับทีม Data Science นำไปทำ Model ทำนายค่าใช้จ่ายผู้ป่วย (Medical Cost Prediction) เพื่อกำหนดราคาเบี้ยประกัน (Premium Pricing) ที่แม่นยำ
* **✅ Actuarial Data Quality:** ใช้ Dagster และ DBT Tests ควบคุมคุณภาพข้อมูลอย่างเข้มงวด เพราะข้อมูลที่ผิดพลาดหมายถึงการประเมินความเสี่ยงที่ผิดพลาด

## 🏗️ Architecture & Data Lineage

![Global Asset Lineage](images/Global_Asset_Lineage.svg)
แผนภาพแสดงการไหลของข้อมูล (Data Flow) ทั้งหมดในระบบ ควบคุมโดย **Dagster**

ระบบทำงานโดยรับข้อมูลดิบให้เป็น Insight ผ่านกระบวนการ ETL 3 Layers:
1.  **Ingestion Layer (Python & Pandas):** Load to PostgreSQL & Validate raw data `raw_medical_insurance`.
2.  **Transformation Layer (dbt):**
    * **Staging:** แปลงข้อมูลจาก Raw เป็น Staging (`stg_medical_insurance`)
    * **Marts:** สร้าง Dimension และ Fact tables (`dim_patients`, `fct_financial`, etc.) เพื่อพร้อมสำหรับการวิเคราะห์
3.  **Orchestration (Dagster):**
    * ควบคุม dependency ระหว่าง Python assets และ dbt models
    * จัดการ Schedule และ Monitoring

### Pipeline Breakdown

จากแผนภาพ Lineage ระบบแบ่งการทำงานออกเป็น 3 Layer หลัก:

1.  **Ingestion Layer (Python/Pandas):**
    * Asset: `raw_medical_insurance`
    * ทำหน้าที่อ่านไฟล์ CSV, ตรวจสอบ Schema เบื้องต้น และโหลดข้อมูลเข้าสู่ PostgreSQL (Raw Layer) โดยใช้ Pandas จัดการ Logic การเขียนข้อมูล (Append/Replace)

2.  **Staging Layer (dbt):**
    * Asset: `stg_medical_insurance`
    * ทำหน้าที่ Data Cleaning, Casting Type และ Standardize ชื่อคอลัมน์
    * **Data Quality:** มีการใช้ **Dagster Asset Checks** ร่วมกับ dbt tests เพื่อตรวจสอบคุณภาพข้อมูล (เช่น `not_null`, `unique`) ซึ่งในภาพแสดงสถานะ **"7/7 Passed"** ✅

3.  **Mart Layer (dbt):**
    * Modeling ข้อมูลให้อยู่ในรูปแบบ **Star Schema** เพื่อประสิทธิภาพในการทำ Analytics
    * **Dimension Tables:** `dim_patients`, `dim_insurance_plan`, `dim_medical_history`
    * **Fact Table:** `fct_financial_transaction` (เก็บ Transaction การเบิกจ่ายจริง)

## 🏛️ Project Structure
```bash
.
├── medical_cost_etl/      # Dagster Code (Assets, Definitions)
│   ├── assets/
│   │   ├── ingestion.py   # Python Logic for Raw Data
│   │   └── dbt_assets.py  # Dagster-DBT Integration
│   └── definitions.py     # Main Entry Point
├── medical_cost_dbt/      # DBT Project
│   ├── models/            # SQL Transformations
│   │   ├── staging/
│   │   │    ├── src_med_insure.yml
│   │   │    ├── stg_med_insure.yml       
│   │   │    └── stg_medical_insurance.sql
│   │   └── marts/
│   │       └── core/
│   │           ├── dim_patients.sql
│   │           ├── dim_medical_history.sql
│   │           └── fct_financial_transactions.sql
│   ├── seeds/
│   ├── dbt_project.yml
│   └── profiles.yml
├── data/                  # Raw Data
├── docker-compose.yml     # Database Config
├── setup.py
└── scripts/
    └── mock_pii_data.py
```

## 🛠️ Tech Stack

* **Language:** Python 3.11.9
* **Orchestrator:** Dagster
* **Transformation:** dbt (Data Build Tool)
* **Database:** PostgreSQL
* **Containerization:** Docker & Docker Compose

## 🚀 Getting Started

### 1. Prerequisites
ต้องติดตั้งโปรแกรมเหล่านี้ก่อน:
* [Docker Desktop](https://www.docker.com/) (สำหรับ Database)
* Python 3.10+
* Git

### 2. Installation

Clone โปรเจกต์และติดตั้ง dependencies:

```bash
# 1. Clone repo
git clone [https://github.com/PrintTrd/insurance_loss_ratio.git](https://github.com/PrintTrd/insurance_loss_ratio.git)
cd insurance_loss_ratio

# 2. สร้างและ Activate Virtual Environment
python -m venv venv
python -m pip install --upgrade pip
# Windows:
.\venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 3. ติดตั้ง Library (Editable mode)
pip install -e .

# 4. Run Database - PostgreSQL
docker-compose up -d

# 5. Run Pipeline - Dagster UI
cd medical_cost_dbt
dbt parse
cd ..
dagster dev -m medical_cost_etl.definitions

เปิด Browser ไปที่: http://localhost:3000
```

