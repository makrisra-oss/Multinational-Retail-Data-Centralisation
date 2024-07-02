import pandas as pd
import numpy as np
import re
from datetime import datetime

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        df = df.copy()

        df.dropna(how='all', inplace=True)

        date_columns = ['date_of_birth', 'join_date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        df['phone_number'] = df['phone_number'].apply(self.clean_phone_number)

        df['email_address'] = df['email_address'].apply(self.clean_email)

        df['country_code'] = df['country_code'].apply(self.clean_country_code)

        current_year = datetime.now().year
        df = df[(df['date_of_birth'].dt.year > current_year - 120) &
                (df['date_of_birth'].dt.year < current_year)]
        
        df = df[df['join_date'] <= pd.Timestamp.now()]

        df['address'] = df['address'].apply(self.clean_address)

        df.dropna(inplace=True)

        return df
    
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