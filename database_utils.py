import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
# from data_extraction import DataExtractor
# from data_cleaning import DataCleaning

class DatabaseConnector:
    def __init__(self):
        self.credentials = None
        self.engine = None

    def read_db_creds(self):
        """
        Reads the database credentials from the db_creds.yaml file
        and returns them as a dictionary.
        """
        try:
            with open('db_creds.yaml', 'r') as file:
                self.credentials = yaml.safe_load(file)
            return self.credentials
        except FileNotFoundError:
            print("Error: db_creds.yaml file not found.")
            return None
        except yaml.YAMLError as e:
            print(f"Error reading YAML file: {e}")
            return None

    def init_db_engine(self, local = False):
        """
        Initializes and returns an SQLAlchemy database engine
        using the credentials from read_db_creds.
        """

        self.read_db_creds()

        if local:
            try:
                db_url = f"postgresql://{self.credentials['LOCAL_USER']}:{self.credentials['LOCAL_PASSWORD']}@{self.credentials['LOCAL_HOST']}:{self.credentials['LOCAL_PORT']}/{self.credentials['LOCAL_DATABASE']}"
                self.engine = create_engine(db_url)
                print("Database engine initialized successfully.")
                return self.engine
            except Exception as e:
                print(f"Error initializing database engine: {e}")
                return None
        else:
            if self.credentials:
                try:
                    db_url = f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
                    self.engine = create_engine(db_url)
                    print("Database engine initialized successfully.")
                    return self.engine
                except Exception as e:
                    print(f"Error initializing database engine: {e}")
                    return None
            else:
                print("No credentials available. Cannot initialize database engine.")
                return None
            

    def list_db_tables(self):
        if not self.engine:
            self.init_db_engine()

        if self.engine:
            try:
                inspector = inspect(self.engine)
                tables = inspector.get_table_names()
                print("Tables in the database:")
                for table in tables:
                    print(f"- {table}")
                return tables
            except Exception as e:
                print(f"Error listing database tables: {e}")
                return None
        else:
            print("No database engine available. Cannot list tables.")
            return None

    def upload_to_db(self, df, table_name):
        """
        Uploads a Pandas DataFrame to a specified table in the database.

        :param df: Pandas DataFrame to upload
        :param table_name: Name of the table to upload the data to
        """

        self.init_db_engine(local=True)
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Data successfully uploaded to table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")

    def extract_clean_upload_users(self):
        """
        Extracts user data, cleans it, and uploads it to the 'dim_users' table in the database.
        """
        # Step 1: Extract user data
        print("Extracting user data...")
        user_data = self.data_extractor.retrieve_user_data(self)
        
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

# Example usage:
if __name__ == "__main__":
    connector = DatabaseConnector()
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


    