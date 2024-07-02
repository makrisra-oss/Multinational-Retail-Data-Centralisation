from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def extract_user_data():
    db_utils = DatabaseConnector()
    data_extraction = DataExtractor()

    tables = db_utils.list_db_tables()
    if not tables:
        print("No tables found in the database.")
        return None
    
    user_table = None
    for table in tables:
        if 'user' in table.lower():
            user_table = table
            break
    if not user_table:
        print("User data table not found.")
        return None
    
    user_data_df = data_extraction.read_rds_table(db_utils, user_table)

    if user_data_df is not None:
        print(f"Successfully extracted user data from table: {user_table}")
        print(f"Shape of the DataFrame: {user_data_df.shape}")
        return user_data_df
    else:
        print("Failed to extract user data.")
        return None

if __name__ == "__main__":
    user_df = extract_user_data()
    if user_df is not None:
        print(user_df.head())

cleaner = DataCleaning()
cleaned_user_df = cleaner.clean_user_data(user_df)