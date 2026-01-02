import duckdb
from faker import Faker
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MedicalDataMocker:
    def __init__(self, db_config: str):
        """ Get Connection String"""
        self.db_config = db_config
        self.duck_con = None
        self.fake = Faker('th_TH')
        Faker.seed(42)

    def _get_thai_citizen_id(self):
        return self.fake.ssn()

    def _get_thai_name(self):
        return self.fake.name()

    def _get_thai_address(self):
        return self.fake.address().replace('\n', ' ')

    def _get_thai_phone(self):
        return self.fake.phone_number()
    
    def connect(self):
        try:
            logger.info("Connecting to DuckDB and attaching Postgres...")
            # In-memory connection
            self.duck_con = duckdb.connect()
            self.duck_con.sql("INSTALL postgres; LOAD postgres;")
            self.duck_con.sql(f"ATTACH '{self.db_config}' AS pg_db (TYPE POSTGRES);")
            logger.info("Connection successful.")
        except Exception as error:
            logger.error(f"Connection failed: {error}")
            raise error

    def _register_udfs(self):
        """(Internal Method) register Python functions for SQL to call"""
        logger.info("Registering Faker functions...")

        self.duck_con.create_function("fake_citizen_id", self._get_thai_citizen_id, [], "VARCHAR")
        self.duck_con.create_function("fake_name", self._get_thai_name, [], "VARCHAR")
        self.duck_con.create_function("fake_address", self._get_thai_address, [], "VARCHAR")
        self.duck_con.create_function("fake_phone", self._get_thai_phone, [], "VARCHAR")
        logger.info("UDFs registered successfully.")

    def ensure_schema_exists(self):
        """ Checking schema in Postgres and adding missing columns if not exists """
        logger.info("Checking schema in Postgres...")
        alter_commands = [
            "ALTER TABLE pg_db.public.raw_medical_insurance ADD COLUMN IF NOT EXISTS citizen_id VARCHAR(20)",
            "ALTER TABLE pg_db.public.raw_medical_insurance ADD COLUMN IF NOT EXISTS full_name VARCHAR(255)",
            "ALTER TABLE pg_db.public.raw_medical_insurance ADD COLUMN IF NOT EXISTS address TEXT",
            "ALTER TABLE pg_db.public.raw_medical_insurance ADD COLUMN IF NOT EXISTS phone_number VARCHAR(50)"
        ]
        for cmd in alter_commands:
            try:
                self.duck_con.sql(cmd)
            except Exception as error:
                logger.warning(f"Schema checking: {error}")

    def generate_and_load(self):
        if not self.duck_con:
             self.connect()

        self._register_udfs()
        self.ensure_schema_exists()
        logger.info("Generating data...")

        # Create Temp Table
        self.duck_con.sql("""
            CREATE OR REPLACE TABLE temp_enriched_data AS
            SELECT 
                * EXCLUDE (citizen_id, full_name, address, phone_number),   
                fake_citizen_id() as citizen_id,
                fake_name() as full_name,
                fake_address() as address,
                fake_phone() as phone_number
            FROM pg_db.public.raw_medical_insurance;
        """)
        logger.info("Writing back to Postgres (Overwrite)...")

        # Transaction: Delete Old -> Insert New
        self.duck_con.sql("BEGIN TRANSACTION;")
        self.duck_con.sql("DELETE FROM pg_db.public.raw_medical_insurance;")
        self.duck_con.sql("""
            INSERT INTO pg_db.public.raw_medical_insurance
            SELECT * FROM temp_enriched_data;
        """)
        self.duck_con.sql("COMMIT;")
        
        logger.info("Data has been mocked.") 

    def close(self):
        if self.duck_con:
            self.duck_con.close()
            logger.info("Connection closed.")

if __name__ == "__main__":
    import os
    
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT")
    DB_CONNECTION_STRING = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

    mocker = MedicalDataMocker(DB_CONNECTION_STRING)
    
    try:
        mocker.generate_and_load()
    except Exception as error:
        logger.error(f"Critical Error: {error}")
    finally:
        mocker.close()
