import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
# from data_extraction import DataExtractor
from data_cleaning import DataCleaning

class DatabaseConnector:
    """This class is for connecting to both the external and local databases.

    Attributes:
        engine: connect to database engine.
    """
    
    def __init__(self):
        self.engine = None

    def read_db_creds(self, yaml_file):
        """
        Reads the database credentials from the db_creds.yaml file
        and returns them as a dictionary.
        """
        with open(yaml_file, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def init_db_engine(self, yaml_file):
        """
        Initializes and returns an SQLAlchemy database engine
        using the credentials from read_db_creds.
        """
        credentials = self.read_db_creds(yaml_file)
        engine = create_engine(f"postgresql://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOST']}:{credentials['PORT']}/{credentials['DATABASE']}")
        return engine
            

    def list_db_tables(self):
        """
        List all table names in the connected database.

        Returns:
            list: A list of table names in the database.

        Prints:
            The name of each table in the connected database.
        """
        inspector = inspect(self.init_db_engine("db_creds_RDS.yaml"))
        tables = inspector.get_table_names()
        for table in tables:
        print(f"table names- {table}")
        return tables


    def upload_to_db(self, df, table_name):
        """
        Uploads a Pandas DataFrame to a specified table in the database.

        :param df: Pandas DataFrame to upload
        :param table_name: Name of the table to upload the data to
        """
        # Upload pandas df to db
        df.to_sql(table_name, self.init_db_engine('db_creds_local.yaml'), if_exists='replace')
        result = f"Data successfully uploaded to table '{table_name}'."
        print(result)
        return result

    def extract_clean_upload_users(self):
        """
        Extracts user data, cleans it, and uploads it to the 'dim_users' table in the database.
        """
        # Step 1: Extract user data
        print("Extracting user data...")
        user_data = self.extractor.retrieve_user_data(self)
        if user_data is not None:
            print(f"Retrieved {len(user_data)} rows of user data.")
            # Step 2: Clean user data
            print("Cleaning user data...")
            cleaned_user_data = self.data_cleaner.clean_user_data(user_data)
            print(f"Cleaned data now has {len(cleaned_user_data)} rows.")
            # Step 3: Upload cleaned data to the database
            print("Uploading cleaned user data to database...")
            self.upload_to_db(cleaned_user_data, 'dim_users')
            print("Process completed successfully.")
        else:
            print("Failed to retrieve user data. Process aborted.")

    def clean_and_upload_card_data(self, url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        """
        Extracts card data from a PDF, cleans it, and uploads it to the 'dim_card_details' table in the database.
        
        :param pdf_link: URL or file path of the PDF document containing card data
        """
        # Step 1: Extract card data from PDF
        print("Extracting card data from PDF...")
        card_data = self.data_extractor.retrieve_pdf_data(url)
        if card_data is not None:
            print(f"Retrieved {len(card_data)} rows of card data.")
            # Step 2: Clean card data
            print("Cleaning card data...")
            cleaned_card_data = self.data_cleaner.clean_card_data(card_data)
            print(f"Cleaned data now has {len(cleaned_card_data)} rows.") 
            # Step 3: Upload cleaned data to the database
            print("Uploading cleaned card data to database...")
            self.upload_to_db(cleaned_card_data, 'dim_card_details')
            print("Process completed successfully.")
        else:
            print("Failed to retrieve card data from PDF. Process aborted.")

    def upload_clean_store_data(self, df):
        """
        Method uploads the clean store data to the local database.

        Args: takes a dataframe as a parameter

        Returns: the clean store data database locally. 
        """
        cleaned_store_data = cleaning.clean_store_data(df)
        return cleaned_store_data

# Example usage:
if __name__ == "__main__":
    """
    If name == main:
    Instantiates my classes and runs instances of my methods.
    """
    connector = DatabaseConnector()
    cleaning = DataCleaning()
    engine = connector.init_db_engine("db_creds_local.yaml")
    print(engine)
    table_list = connector.list_db_tables()
    print(f"table list:", table_list)


    