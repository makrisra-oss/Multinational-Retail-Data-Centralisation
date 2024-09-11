import pandas as pd
import requests
import boto3
import io

class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_name):
        if not db_connector.engine:
            db_connector.engine = db_connector.init_db_engine()

        if not db_connector.engine:
            print("Database engine is not initialized.")
            return None
        
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, db_connector.engine)
            return df
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return None

    def read_csv_file(self, file_path):
        
        try:
            df = pd.read_csv(file_path)
            return df
        
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None

    def extract_from_api(self, api_url, headers=None):
        
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error extracting data from API: {e}")
            return None
        
    def extract_from_s3(self, bucket_name, object_key, aws_access_key_id, aws_secret_access_key):

        try:
            s3 = boto3.client('s3',
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
            
            obj = s3.get_object(Bucket=bucket_name, Key=object_key)
            data = obj['Body'].read()

            df = pd.read_csv(io.BytesIO(data))
            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None