import pandas as pd
import numpy as np
import re
from datetime import datetime
import tabula

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, user_df: pd.DataFrame):

        # drop null values if every value n/a
        user_df = user_df.dropna(how='all')
        print(user_df.isnull().sum())

        # drop Unnamed columns
        # df = df.drop('Unnamed: 0', axis=1)

        user_df = user_df.loc[:, ~user_df.columns.str.contains('^Unnamed')]
        print(user_df.columns)
        # replace GGB with GB > incorrectly typed v



        user_df['country_code'] = user_df['country_code'].str.replace('GGB', 'GB')
        print(id(user_df))
        #Rows filled with wrong information
        user_df = user_df[user_df['country_code'].str.len() < 3]

        #drop country_codes with more than 2 characters

        # example_series = example_series.str.replace('@', 'o')
    

        #df.drop(df.loc[df['line_race']==0].index, inplace=True)

        date_columns = ['date_of_birth', 'join_date']
        for col in date_columns:
            user_df[col] = pd.to_datetime(user_df[col], format='%Y-%m-%d', errors='coerce')

        # df = df.drop(df[df.score < 50].index)

        return user_df
    

    def clean_card_data(self, card_df: pd.DataFrame):
        # Remove rows where all values are NULL
        card_df = card_df.dropna(how='all')

        # Remove duplicates
        card_df = card_df.drop_duplicates()
        
        # Ensure Card Numbers Have Valid Length:
        card_df = card_df[card_df['card_number'].astype(str).str.len().between(13, 19)]

        #Remove non-numeric card numbers
        card_df['card_number'] = card_df['card_number'].astype(str)

        card_df = card_df[card_df['card_number'] != 'nan']

        card_df = card_df[card_df['card_number'].str.isdigit()] # # Keep only rows where card_number is numeric

        # Remove rows with missing critical information
        card_df = card_df.dropna(subset=['card_number', 'card_provider', 'expiry_date'])

        # Convert expiration date to datetime and remove expired cards
        card_df['expiry_date'] = pd.to_datetime(card_df['expiry_date'], errors='coerce')
        current_date = pd.to_datetime('today')
        card_df = card_df[card_df['expiry_date'] > current_date]

        # Strip whitespace from text fields
        card_df['card_provider'] = card_df['card_provider'].str.strip()

        #Ensure card_number is a string
        card_df['card_number'] = card_df['card_number'].astype(str)

        return card_df
    

    def clean_store_data(self, store_df: pd.DataFrame):

        # Turn store_df series into df
        store_df = pd.DataFrame(store_df)
        print(type(store_df))

        #Drop 'lat' column
        store_df = store_df.drop(columns=['lat'])
        print(type(store_df))

        pattern = r'^[A-Za-z0-9]{10}$' #Pattern for 10 alphanumeric characters
        store_df = store_df.replace(pattern, value=np.nan, regex=True)
        print(type(store_df))

        #Filters stores which have a number in their index but all other cells are NaN
        store_df = store_df[~(store_df.iloc[:, 1:].isna().all(axis=1) & store_df['index'].apply(lambda x: isinstance(x, (int, float))))]

        #Turn NULL values to NaN values
        store_df = store_df.replace(['NULL', 'N/A'], pd.NA)
        
        # Remove rows where all values are NaN
        store_df = store_df.dropna(how='all')
        print(type(store_df))

        # Remove duplicate rows, if any
        store_df = store_df.drop_duplicates()
        print(type(store_df))

        # Clean the address by removing newlines (\n)
        if 'address' in store_df.columns:
            store_df['address'] = store_df['address'].replace(['\n', r'\\'], ' ', regex=True)
        print(type(store_df))

        if 'continent' in store_df.columns:
            store_df['continent'] = store_df['continent'].replace('eeEurope', 'Europe', regex=True)
            store_df['continent'] = store_df['continent'].replace('eeAmerica', 'America', regex=True)
        print(type(store_df))

        print(type(store_df))
        return store_df
    
    @staticmethod
    def convert_product_weights(product_df):
        converted_weights = []
        
        # Iterate over each weight value in the product_df 'weight' column
        for item in product_df['weight']:
            if isinstance(item, str):
                # Match patterns like '12 x 100' and compute the product
                matches = re.findall(r'(\d+)\s*x\s*(\d+)', item)
                if matches:
                    # Extract the two numbers, compute the product
                    num1, num2 = map(int, matches[0])
                    converted_weights.append(num1 * num2)  # Replace '12 x 100' with 1200
                elif item.endswith('kg'):
                    converted_weights.append(float(item[:-2]))  # Convert 'kg' to grams
                elif item.endswith('k'):
                    converted_weights.append(float(item[:-1]))  # Convert 'k' to grams
                elif item.endswith('g'):
                    converted_weights.append(float(item[:-1]) / 1000)  # Convert 'g' to float
                elif item.endswith('ml'):
                    converted_weights.append(float(item[:-2]) / 1000)  # Convert 'ml' to float
                else:
                    converted_weights.append(np.nan)  # Append NaN for unknown formats
            else:
                converted_weights.append(np.nan)  # Append NaN if item is not a string

        kg_weights = [f"{weight}kg" for weight in converted_weights]

        # Check if the length of converted_weights matches the length of product_df
        if len(converted_weights) != len(product_df):
            raise ValueError(f"Length of converted_weights ({len(converted_weights)}) does not match length of product_df ({len(product_df)})")

        # Assign the cleaned weights back to the DataFrame
        product_df['weight'] = kg_weights
        
        return product_df
    
    def clean_products_data(self, products_df):
        # Remove rows where all values are Null
        products_df = products_df.dropna(how='all')
        
        products_df = products_df.loc[:, ~products_df.columns.str.contains('Unnamed')]

        # Remove rows where the weight column contains 'nankg'
        products_df = products_df[~products_df.isin(['nankg']).any(axis=1)]

        # Remove duplicates
        products_df = products_df.drop_duplicates()

        # Create a new list for the 'removed' column
        new_removed = []

        for item in products_df['removed']:
            # Correct the typo "Still_avaliable" to "Still_available"
            if item == "Still_avaliable":  
                new_removed.append("Still_available")
            else:
                new_removed.append(item)

        # Assign the corrected 'removed' list back to the original DataFrame
        products_df['removed'] = new_removed

        new_product_names = []

        for item in products_df['product_name']:
            if isinstance(item, str):
                new_item = item.strip('"')
                new_product_names.append(new_item)
            else:
                new_product_names.append(item)

        products_df['product_name'] = new_product_names

        products_df = self.convert_product_weights(products_df) #assigning clean weights to the products_df

        return products_df
        # Return the cleaned DataFrame
    
    @staticmethod
    def clean_phone_number(phone):
        if pd.isna(phone):
            return np.nan
        
        phone = re.sub(r'\D', '', str(phone))

        if 10 <= len(phone) <= 15:
            return phone
        return np.nan
    
    @staticmethod
    def clean_email(email):
        if pd.isna(email):
            return np.nan
        
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(email_regex, str(email)):
            return email.lower()
        return np.nan
    
    @staticmethod
    def clean_country_code(code):
        if pd.isna(code):
            return np.nan
        
        code = str(code).upper().strip()
        if len(code) == 2 and code.isalpha():
            return code
        return np.nan
    
    @staticmethod
    def clean_address(address):
        if pd.isna(address):
            return np.nan
        
        address = re.sub(r'\s+', '', str(address).replace('\n', '')).strip()
        return address if address else np.nan

if __name__ == "__main__":
    from data_extraction import DataExtractor

    extractor = DataExtractor()
    cleaning = DataCleaning()

    raw_user_df = pd.read_csv('users_table.csv')
    raw_card_df = extractor.retrieve_pdf_data(pdf_link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    raw_store_df = extractor.retrieve_stores_data(endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}")
    raw_product_df = extractor.extract_from_s3(bucket_name='data-handling-public', s3_key='products.csv', local_path='/opt/homebrew/Caskroom/miniconda/base/envs/mrdc/local_products.csv')

    cleaned_weights_df = cleaning.convert_product_weights(raw_product_df)

    print(cleaning.clean_user_data(raw_user_df))
    print(cleaning.clean_card_data(raw_card_df))
    print(raw_card_df.columns)
    print(cleaning.clean_store_data(raw_store_df).columns)
    print(raw_store_df.columns)
    print(cleaned_weights_df['weight'].head())
    print(raw_product_df.columns)
    print(cleaning.clean_products_data(raw_product_df))