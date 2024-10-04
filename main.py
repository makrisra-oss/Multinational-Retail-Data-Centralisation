from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd

connector = DatabaseConnector()
extractor = DataExtractor()
cleaning = DataCleaning()


def user_pipeline():
    connector_RDS = DatabaseConnector()
    engine_RDS = connector_RDS.init_db_engine("db_creds_RDS.yaml")
    if engine_RDS:
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
    card_table = extractor.retrieve_pdf_data(pdf_link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    store_table = extractor.retrieve_stores_data(endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}")
    products_table = extractor.extract_from_s3(bucket_name='data-handling-public', object='products.csv', file_name='local_products.csv')
    orders_table = extractor.read_rds_table(connector, table_name='orders_table')
    date_events_table = pd.DataFrame(extractor.retrieve_json_data(url="https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"))

    user_table.to_csv('user_table.csv')
    card_table.to_csv('card_table.csv')
    store_table.to_csv('store_table.csv')
    products_table.to_csv('products_table.csv')
    orders_table.to_csv('orders_table.csv')
    date_events_table.to_csv('date_events_table.csv')

    user_table = pd.read_csv('user_table.csv')
    card_table = pd.read_csv('card_table.csv')
    store_table = pd.read_csv('store_table.csv')
    products_table = pd.read_csv('products_table.csv')
    orders_table = pd.read_csv('orders_table.csv')
    date_events_table = pd.read_csv('date_events_table.csv')

    cleaned_user_data = cleaning.clean_user_data(user_table)
    cleaned_card_data = cleaning.clean_card_data(card_table)
    cleaned_store_data = cleaning.clean_store_data(store_table)
    cleaned_products_data = cleaning.clean_products_data(products_table)
    cleaned_orders_data = cleaning.clean_orders_data(orders_table)
    cleaned_date_events_data = cleaning.clean_date_events_data(date_events_table)

    connector_local = DatabaseConnector()
    engine_local = connector_local.init_db_engine("db_creds_local.yaml")

    if engine_local:
        print("Database engine created successfully.")
    # You can now use this engine to interact with your database
    else:
        print("Failed to create database engine.")

    upload_result = connector_local.upload_to_db(cleaned_user_data, "dim_users")
    upload_result = connector_local.upload_to_db(cleaned_card_data, "dim_card_details")
    upload_result = connector_local.upload_to_db(cleaned_store_data, "dim_store_details")
    upload_result = connector_local.upload_to_db(cleaned_products_data, "dim_products")
    upload_result = connector.upload_to_db(cleaned_orders_data, "orders_table")
    upload_result = connector.upload_to_db(cleaned_date_events_data, "dim_date_times")

    print(f"test: {type(cleaned_user_data)}")
    print(len(cleaned_user_data))
    print(cleaned_user_data.columns)
    print(cleaned_user_data['country_code'].unique())

    print(f"test: {type(cleaned_card_data)}")
    print(len(cleaned_card_data))
    print(cleaned_card_data.columns)

    print(f"test: {type(cleaned_store_data)}")
    print(len(cleaned_store_data))
    print(cleaned_store_data.columns)

    print(f"test: {type(cleaned_products_data)}")
    print(len(cleaned_products_data))
    print(cleaned_products_data)

    print(f"test: {type(cleaned_orders_data)}")
    print(len(cleaned_orders_data))
    print(cleaned_orders_data)

    print(f"test: {type(cleaned_date_events_data)}")
    print(len(cleaned_date_events_data))
    print(cleaned_date_events_data)
    
    print(upload_result)

user_pipeline()
