import pandas as pd
from sqlalchemy import create_engine

def upload_to_db(data, table_name, connection_string):
    engine = create_engine(connection_string)
    data.to_sql(table_name, engine, if_exists='replace', index=False)

def main():
    # Here you would put your data extraction and cleaning code
    # For example:
    # cleaned_data = extract_and_clean_data()

    connection_string = "postgresql://username:password@localhost/sales_data"

    upload_to_db(cleaned_data, 'dim_users', connection_string)

if __name__ == "__main__":
    main()
