from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

connector = DatabaseConnector()
extractor = DataExtractor()
cleaning = DataCleaning()
engine = connector.init_db_engine()
if engine:
    print("Database engine created successfully.")
    # You can now use this engine to interact with your database
else:
    print("Failed to create database engine.")

tables = connector.list_db_tables()
if tables:
    print(f"Successfully retrieved {len(tables)} tables.")
else:
    print("Failed to retrieve database tables.")

user_table = extractor.read_rds_table(connector, "legacy_users")
print(user_table)

clean_user_table = cleaning.clean_user_data(user_table)
print(clean_user_table)

connector.upload_to_db(clean_user_table, "dim_users")