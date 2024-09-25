from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

connector = DatabaseConnector()
extractor = DataExtractor()
cleaning = DataCleaning()
# engine = connector.init_db_engine("db_creds_RDS.yaml")
# if engine:
#     print("Database engine created successfully.")
#     # You can now use this engine to interact with your database
# else:
#     print("Failed to create database engine.")

# tables = connector.list_db_tables()
# if tables:
#     print(f"Successfully retrieved {len(tables)} tables.")
# else:
#     print("Failed to retrieve database tables.")

# user_table = extractor.read_rds_table(connector, "legacy_users")
# print(user_table)

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

    # user_table = extractor.read_rds_table(connector, "legacy_users")
    # card_table = extractor.retrieve_pdf_data(pdf_link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    # store_table = extractor.retrieve_stores_data(endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}")
    products_table = extractor.extract_from_s3(bucket_name='data-handling-public', s3_key='products.csv', local_path='/opt/homebrew/Caskroom/miniconda/base/envs/mrdc/local_products.csv')
    raw_product_df = extractor.extract_from_s3(bucket_name='data-handling-public', s3_key='products.csv', local_path='/opt/homebrew/Caskroom/miniconda/base/envs/mrdc/local_products.csv')

    # cleaned_user_data = cleaning.clean_user_data(user_table)
    # cleaned_card_data = cleaning.clean_card_data(card_table)
    # cleaned_store_data = cleaning.clean_store_data(store_table)
    cleaned_products_data = cleaning.clean_products_data(products_table)

    connector_local = DatabaseConnector()

    engine_local = connector_local.init_db_engine("db_creds_local.yaml")

    if engine_local:
        print("Database engine created successfully.")
    # You can now use this engine to interact with your database
    else:
        print("Failed to create database engine.")

    # upload_result = connector_local.upload_to_db(cleaned_user_data, "dim_users")
    # upload_result = connector_local.upload_to_db(cleaned_card_data, "dim_card_details")
    # upload_result = connector_local.upload_to_db(cleaned_store_data, "dim_store_details")
    upload_result = connector_local.upload_to_db(cleaned_products_data, "dim_products")

    # print(f"test: {type(cleaned_user_data)}")
    # print(len(cleaned_user_data))
    # print(cleaned_user_data.columns)
    # print(cleaned_user_data['country_code'].unique())

    # print(f"test: {type(cleaned_card_data)}")
    # print(len(cleaned_card_data))
    # print(cleaned_card_data.columns)

    # print(f"test: {type(cleaned_store_data)}")
    # print(len(cleaned_store_data))
    # print(cleaned_store_data.columns)

    print(f"test: {type(cleaned_products_data)}")
    print(len(cleaned_products_data))
    print(cleaned_products_data)
    
    
    print(upload_result)
    # print(cleaning.clean_user_data.unique())

    

user_pipeline()





# clean_user_table = cleaning.clean_user_data(user_table)
# print(clean_user_table)

# connector.upload_to_db(clean_user_table, "dim_users")