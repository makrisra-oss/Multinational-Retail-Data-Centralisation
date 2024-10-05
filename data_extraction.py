import pandas as pd
import requests
import boto3
import io
from database_utils import DatabaseConnector
import tabula
import urllib.request, json

class DataExtractor:
    """
    DataExtractor class:
        Extracts data from the various data sources.
    """
    def __init__(self):
        """
        Initialises the class instance.

        Placeholder constructor that does not perform any specific initialisation.
        """
        pass

    def read_rds_table(self, db_connector, table_name, yaml_file):
        """
        Read a table from an RDS (Relational Database Service) instance into a pandas DataFrame.

        This method initializes a database engine using the given YAML file for credentials,
        connects to the specified RDS table, and retrieves the entire table content as a pandas DataFrame.

        Args:
            db_connector (object): An instance of the database connector class that manages database connections.
            table_name (str): The name of the table to read from the RDS database.
            yaml_file (str): Path to the YAML file containing database connection credentials.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the specified RDS table.
        """
        db_connector.engine = db_connector.init_db_engine(yaml_file)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, db_connector.engine)
        return df

    def read_csv_file(self, file_path):
        """
        Read a CSV file into a pandas DataFrame.

        This method attempts to read a CSV file from the given file path and loads its contents 
        into a pandas DataFrame. If an error occurs during the file reading process, it catches 
        the exception, prints an error message, and returns `None`.

        Args:
            file_path (str): The file path to the CSV file to be read.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the CSV file, or `None` if an error occurs.

        Raises:
            Exception: Catches any exception that may occur during the file reading process and prints it.
        """
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None

    def extract_from_api(self, api_url, headers=None):
        """
        A method to extract from an API url.

        Args:
            api_url
            headers

        Returns:
            JSON data
        """
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error extracting data from API: {e}")
            return None

    def extract_from_s3(self, bucket_name, object, file_name):
        """
        A method to extract from the s3 bucket.

        Args:
            bucket_name
            object
            file_name

        Returns:
            dataframe
        """
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, object, file_name)
        df = pd.read_csv(file_name)
        return df

    def retrieve_json_data(self, url):
        with urllib.request.urlopen("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json") as url:
            df = json.load(url)
        return df
        
    def retrieve_pdf_data(self, pdf_link):
        """
        Extracts data from a PDF file and returns it as a pandas DataFrame.
        
        :param pdf_link: URL or file path of the PDF
        :return: pandas DataFrame containing the extracted data
        """
        tables = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        # Combine all tables into a single DataFrame
        if tables:
            combined_df = pd.concat(tables, ignore_index=True)
            return combined_df
        else:
            return pd.DataFrame()  # Return an empty DataFrame if no tables found

    @staticmethod
    def list_number_of_stores(endpoint, headers):
        """
        Retrieves the number of stores from the API.
            
        :param endpoint: API endpoint URL for retrieving the number of stores
        :param headers: Dictionary containing the API key
        :return: Number of stores (int) or None if the request fails
        """
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status() # Raises an HTTPError for bad responses
        data = response.json()
        print(data)
        return data['number_stores']
    
    @staticmethod
    def retrieve_stores_data(endpoint, yaml_file):
        """
        A method for retrieving store data from an API key using a yaml_file in this instance

        Args:
            endpoint
            yaml_file
        
        Returns:
            pandas dataframe
        """
        # Create another method retrieve_stores_data which will take the retrieve a store endpoint as an argument
        # and extracts all the stores from the API saving them in a pandas DataFrame.
        store_data_list = []
        store_numbers = list(range(0, 451))
        for store_number in store_numbers:
                api_url = endpoint.format(store_number=store_number)  
                headers_1 = yaml_file
                response = requests.get(api_url, headers=headers_1)
                response.raise_for_status() #HTTP error for bad responses
                print(response)
                data = response.json()
                store_data_list.append(data)
                print(data)

        df = pd.DataFrame(store_data_list)
        return df
           
if __name__ == "__main__":
    """
    name == main:
        Instantiates classes and calls methods with print outs for clarity.
    """
    extractor = DataExtractor()
    connector = DatabaseConnector()
    
    # API endpoints and headers
    retrieve_a_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = connector.read_db_creds("api_key.yaml")

    # Retrieve a store
    store_df = extractor.retrieve_stores_data(retrieve_a_store_endpoint, yaml_file=connector.read_db_creds("api_key.yaml"))
    print(store_df)
    store_numbers = list(range(1, 452))

    # Retrieve number of stores
    num_stores = extractor.list_number_of_stores(number_stores_endpoint, headers)
    print(num_stores)

    # #Retrieve and read RDS table legacy_users
    read_rds_users = extractor.read_rds_table(db_connector=connector, table_name='legacy_users', yaml_file="db_creds_RDS.yaml")
    print(read_rds_users)

    # #Retrieve and read RDS table orders
    read_rds_orders = extractor.read_rds_table(db_connector=connector, table_name='orders_table', yaml_file="db_creds_RDS.yaml")
    print(f"Read RDS Orders: ", read_rds_orders)

    # # # #Retreive PDF data
    return_pdf = extractor.retrieve_pdf_data(pdf_link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(return_pdf)
    
    # #Retrieve AWS csv
    product_df = extractor.extract_from_s3(bucket_name='data-handling-public', object='products.csv', file_name='local_products.csv')
    print(product_df)
    
    #Retrieve date_events
    date_events_df = extractor.retrieve_json_data(url='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    print(date_events_df)
    
