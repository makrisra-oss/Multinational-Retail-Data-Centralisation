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
        #drop country_codes with more than 2 characters
        user_df = user_df[user_df['country_code'].str.len() < 3]

        #df.drop(df.loc[df['line_race']==0].index, inplace=True)
        date_columns = ['date_of_birth', 'join_date']
        for col in date_columns:
            user_df[col] = pd.to_datetime(user_df[col], format='%Y-%m-%d', errors='coerce')

        # df = df.drop(df[df.score < 50].index)
        return user_df
    

    def clean_card_data(self, card_df: pd.DataFrame):
        # print(f"fk value: ", card_df.isin(['4971858637664481']))

        # result = card_df.query('card_number == "4971858637664481"')

        # print(f"Result of query: ", result)

        
        # Remove rows where all values are NULL
        card_df = card_df.dropna(how='all')

        # Remove duplicates
        card_df = card_df.drop_duplicates()
        
        # # Ensure Card Numbers Have Valid Length:
        # card_df = card_df[card_df['card_number'].astype(str).str.len().between(13, 19)]

        # Replace ? with ''
        card_df['card_number'] = card_df['card_number'].replace('\\?', '', regex=True)


        # Remove rows with missing critical information
        # card_df = card_df.dropna(subset=['card_number', 'card_provider', 'expiry_date'])

        # Convert expiration date to datetime and remove expired cards
        # card_df['expiry_date'] = pd.to_datetime(card_df['expiry_date'], errors='coerce')
        # current_date = pd.to_datetime('today')
        # card_df.loc[card_df['expiry_date'] <= current_date, 'expiry_date'] = np.nan

        # Strip whitespace from text fields
        # card_df['card_provider'] = card_df['card_provider'].str.strip()

        # Correct date_payment_confirmed
        """"""
        print(f"type date_payment_confirmed: ", type(card_df['date_payment_confirmed']))

        valid_pattern = r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'

        card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].mask(~card_df['date_payment_confirmed'].str.contains(valid_pattern, na=False))

        invalid_pattern = r'^[A-Z0-9]{10}$'

        card_df = card_df[~card_df['card_number'].str.match(invalid_pattern, na=False)]

        card_df = card_df.drop('Unnamed: 0', axis=1)

        card_df = card_df.dropna(how='all', subset=card_df.columns.difference(['index']))

        # #Drop nan and duplicates for card_number column
        # card_df = card_df.dropna(subset=['card_number'])
        # card_df = card_df.drop_duplicates(subset=['card_number'])

        # #Ensure card_number is a string
        # card_df['card_number'] = card_df['card_number'].astype(str)
        """"""
        return card_df
    

    def clean_store_data(self, store_df: pd.DataFrame):

        # Turn store_df series into df
        store_df = pd.DataFrame(store_df)
        print(type(store_df))

        #Drop 'lat' column
        # store_df = store_df.drop(columns=['lat'])
        # print(type(store_df))

        pattern = r'^[A-Za-z0-9]{10}$' #Pattern for 10 alphanumeric characters
        store_df = store_df.replace(pattern, value=np.nan, regex=True)
        print(type(store_df))

        #Drop str values in store_number
        store_df['staff_numbers'] = store_df['staff_numbers'].str.replace(r'[^0-9]', '', regex=True)

        #Filters stores which have a number in their index but all other cells are NaN
        store_df = store_df[~(store_df.iloc[:, 1:].isna().all(axis=1) & store_df['index'].apply(lambda x: isinstance(x, (int, float))))]

        store_df = store_df.drop('Unnamed: 0', axis=1)

        #Turn NULL values to NaN values
        store_df = store_df.replace(['NULL', 'N/A', ''], pd.NA)
        
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

        store_df = store_df.dropna(how='all', subset=store_df.columns.difference(['index']))

        # Remove rows where all values are NaN
        store_df = store_df.dropna(how='all')
        print(type(store_df))

        # Remove duplicate rows, if any
        store_df = store_df.drop_duplicates()
        print(type(store_df))

        print(f"These are the store columns: ", store_df.columns)

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
                    converted_weights.append((num1 * num2) / 1000)  # Replace '12 x 100' with 1200
                elif item.endswith('kg'):
                    converted_weights.append(float(item[:-2]))  # Convert 'kg' to grams
                elif item.endswith('k'):
                    converted_weights.append(float(item[:-1]))  # Convert 'k' to grams
                elif item.endswith('g'):
                    converted_weights.append(float(item[:-1]) / 1000)  # Convert 'g' to float
                elif item.endswith('ml'):
                    converted_weights.append(float(item[:-2]) / 1000)  # Convert 'ml' to float
                elif item.endswith('oz'):
                    converted_weights.append(float(item[:-2]) * (28.35/1000))
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
        
         # Invalid pattern to identify strings with unwanted characters around the weight
        invalid_pattern = r'\s*\.?\s*(\w+\.?\w+)\s*\.?\s*'
        
        # Correctly formatted pattern that we want to achieve
        valid_pattern = r'\1'  # Use the captured group to keep only the core weight

        products_df['weight'] = products_df['weight'].str.replace(invalid_pattern, valid_pattern, regex=True)
        # Remove rows where all values are Null
        products_df = products_df.dropna(how='all')
        print(f"products df 1: {products_df}")
        products_df = products_df.loc[:, ~products_df.columns.str.contains('Unnamed')]
        print(f"products df 2: {products_df}")
        # Remove rows where the weight column contains 'nankg'
        # products_df = products_df[products_df['weight'].notna() & (products_df['weight'] != '')]

        print(f"Nankg row printed: ", products_df)


        #Remove nonsense string names
        # products_df = products_df[~products_df['product_name'].str.match(r'^[A-Za-z]+[0-9]+$', na=False)]

        # nonsense_mask = products_df['product_name'].str.contains(r'^[A-Za-z0-9]+$', na=False)
        # products_df = products_df[~nonsense_mask]

        # Create the mask to identify nonsense product names
        # nonsense_mask = products_df['product_name'].str.contains(r'^[A-Za-z0-9]+$', na=False)

        # # Use .mask() to replace rows where the condition is True with NaN, and then drop NaN rows
        # products_df = products_df.mask(nonsense_mask).dropna()

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

        products_df = products_df.dropna(how='all', subset=products_df.columns.difference(['index', 'weight']))

        #Get rid of nonsense codes

        invalid_pattern = r'^[A-Z0-9]{10}$'

        products_df = products_df[~products_df['product_price'].str.match(invalid_pattern, na=False)]

        # Comment out if necessary
        products_df = products_df.dropna(how='all')
        products_df = products_df.drop_duplicates()

        return products_df

    def clean_orders_data(self, orders_df):
        #Drop rows which are NULL
        orders_df = orders_df.dropna(how='all')
        print(orders_df)
        #Drop duplicates
        orders_df = orders_df.drop_duplicates()
        #Drop columns
        orders_df = orders_df.drop(columns=['first_name'])
        orders_df = orders_df.drop(columns=['last_name'])
        orders_df = orders_df.drop(columns=['1'])
        # orders_df = orders_df.drop(columns=['Unnamed: 0'])
        orders_df = orders_df.drop(columns=['level_0'])
        
        # Ensure Card Numbers Have Valid Length:
        # orders_df = orders_df[orders_df['card_number'].astype(str).str.len().between(13, 19)]

        # #Ensure card_number is a string
        # orders_df['card_number'] = orders_df['card_number'].astype(str)

        return orders_df
    
    def clean_date_events_data(self, date_events_df):
        #Replace nonsense strings with nan
        valid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    
        # date_events_df = date_events_df[~date_events_df['date_uuid'].str.contains(valid_pattern, na=False)]
        print(f"is na sum before: ", date_events_df['date_uuid'].isna().sum())
        # print(f"is na sum before: ", date_events_df)

        date_events_df = date_events_df.where(date_events_df['date_uuid'].str.contains(valid_pattern))
        
        print(f"is na sum after: ", date_events_df['date_uuid'].isna().sum())
        # print(f"is na sum after: ", date_events_df)

        #Match day, month, year:

        # day_pattern = r'^[0-9]{1,2}$'

        # date_events_df = date_events_df.where(date_events_df['day'].str.contains(day_pattern))

        # month_pattern = r'^[0-9][0-2]{1}$'

        # date_events_df = date_events_df.where(date_events_df['month'].str.contains(month_pattern))

        # year_pattern = r'^[1-2][0-9]{3}$'

        # date_events_df = date_events_df.where(date_events_df['year'].str.contains(year_pattern))
        
        #Drop na
        date_events_df = date_events_df.dropna(how='all')
        print(f"The date events data: ", date_events_df)
        #Drop duplicates
        date_events_df = date_events_df.drop_duplicates()

        return date_events_df
        

if __name__ == "__main__":
    from data_extraction import DataExtractor

    extractor = DataExtractor()
    cleaning = DataCleaning()

    # raw_user_df = pd.read_csv('users_table.csv')
    # raw_card_df = extractor.retrieve_pdf_data(pdf_link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    # raw_store_df = extractor.retrieve_stores_data(endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}")
    # raw_product_df = extractor.extract_from_s3(bucket_name='data-handling-public', object='products.csv', file_name='local_products.csv')
    # raw_orders_df = pd.read_csv('orders_table.csv')
    # raw_date_events_df = pd.DataFrame(extractor.retrieve_json_data(url='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'))

    raw_user_df = pd.read_csv('user_table.csv')
    raw_card_df = pd.read_csv('card_table.csv')
    raw_store_df = pd.read_csv('store_table.csv')
    raw_product_df = pd.read_csv('products_table.csv')
    raw_orders_df = pd.read_csv('orders_table.csv')
    raw_date_events_df = pd.read_csv('date_events_table.csv')

    cleaned_weights_df = cleaning.convert_product_weights(raw_product_df)

    print(cleaning.clean_user_data(raw_user_df))
    print(cleaning.clean_card_data(raw_card_df))
    print(raw_card_df.columns)
    print(cleaning.clean_store_data(raw_store_df).columns)
    print(raw_store_df.columns)
    print(cleaned_weights_df['weight'].head())
    print(raw_product_df.columns)
    print(cleaning.clean_products_data(raw_product_df))
    print(cleaning.clean_orders_data(raw_orders_df))
    print(raw_orders_df.columns)
    print(cleaning.clean_date_events_data(raw_date_events_df))
    print(raw_date_events_df.columns)