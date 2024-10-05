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


    