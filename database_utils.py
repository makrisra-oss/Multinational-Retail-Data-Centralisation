import psycopg2
from psycopg2 import sql
import pandas as pd
from sqlalchemy import create_engine
import yaml

class DatabaseConnector:
    def __init__(self, host, database, user, password, port=5433):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.conn = None
        self.cursor = None
        self.engine = None

        self.creds = self.read_db_creds()

    def read_db_creds(self):
        try:
            with open('db_creds.yaml', 'r') as file:
                creds = yaml.safe_load(file)
            return creds
        except FileNotFoundError:
            print("Credentials file 'db_creds.yaml' not found.")
            return None
        except yaml.YAMLError as e:
            print(f"Error reading YAML file: {e}")
            return None
        
    def init_db_engine(self):
        if not self.creds:
            print("Database credentials are not available.")
            return None
        
        try:
            db_url = f"postgresql://{self.creds['user']}:{self.creds['password']}@{self.creds['host']}:{self.creds['port']}/{self.creds['database']}"
            self.engine = create_engine(db_url)
            return self.engine
        except KeyError as e:
            print(f"Missing credential: {e}")
            return None
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            return None
        
    def list_db_tables(self):
        if not self.engine:
            self.engine = self.init_db_engine()

        if not self.engine:
            print("Database engine is not initialized.")
            return None
        
        try:
            inspector = inspector(self.engine)
            return inspector.get_table_names()
        except Exception as e:
            print(f"Error listing database tables: {e}")
            return None
        
        
        


    def connect(self):
        """
        Establishes a connection to the database.
        """
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.conn.cursor()
            print("Connected to the database successfully.")
        except (Exception, psycopg2.Error) as error:
            print(f"Error while connecting to PostgreSQL: {error}")

    def create_table(self, table_name, columns):
        """
        Creates a new table in the database.

        Args:
            table_name (str): Name of the table to create.
            columns (dict): Dictionary of column names and their data types.
        """
        try:
            columns_str = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
            create_table_query = sql.SQL(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})")
            self.cursor.execute(create_table_query)
            self.conn.commit()
            print(f"Table '{table_name}' created successfully.")
        except (Exception, psycopg2.Error) as error:
            print(f"Error creating table: {error}")

    def upload_data(self, table_name, data):
        """
        Uploads data to the specified table.

        Args:
            table_name (str): Name of the table to upload data to.
            data (pandas.DataFrame): Data to be uploaded.
        """
        try:
            if self.engine is None:
                self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
            
            data.to_sql(table_name, self.engine, if_exists='append', index=False)
            print(f"Data uploaded to table '{table_name}' successfully.")
        except Exception as error:
            print(f"Error uploading data: {error}")

    def execute_query(self, query):
        """
        Executes a SQL query.

        Args:
            query (str): SQL query to execute.

        Returns:
            list: Result of the query.
        """
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print(f"Error executing query: {error}")
            return None

    def close_connection(self):
        """
        Closes the database connection.
        """
        if self.conn:
            if self.cursor:
                self.cursor.close()
            self.conn.close()
            print("Database connection closed.")

# Example usage:
# db = DatabaseConnector(host='localhost', database='sales_data', user='your_username', password='your_password')
# db.connect()
# db.create_table('customers', {'id': 'SERIAL PRIMARY KEY', 'name': 'VARCHAR(100)', 'email': 'VARCHAR(100)'})
# data = pd.DataFrame({'name': ['John Doe', 'Jane Smith'], 'email': ['john@example.com', 'jane@example.com']})
# db.upload_data('customers', data)
# results = db.execute_query("SELECT * FROM customers")
# print(results)
# db.close_connection()