# Multinational-Retail-Data-Centralisation
Project

My project takes 6 tables from various sources extracts (DataExtractor) them efficiently, transforms (DataCleaning) them then loads them to the Postgres database (DatabaseConnector) all coming together in an efficient pipeline in main.py.

The six tables are uploaded to the Postgres database as follows:
-dim_card_details
-dim_date_times
-dim_products
-dim_store_details
-dim_users
-orders_table

The files have already been set up to be used correctly. You can first run the database_utils.py file to connect to the external databases, then use the data_extraction.py file to extract and retrieve data, then run data_cleaning.py to transform the data and clean it, whilst, finally, uploading it to the Postgres db by running the main.py (pipleine) file to view and access the data in SQL

SQL commands added to clean the data somewhat and match the dim tables with primary keys to the orders_table with foreign key. After going through and debugging some filtering code (uncommenting) I was able to complete my SQL tasks. The tables have the correct row quantities with the orders tables and the SQL aspect of the project was, eventually, successful.

All SQL queries carried out and functional. Project complete.