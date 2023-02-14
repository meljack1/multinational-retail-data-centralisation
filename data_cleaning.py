import data_extraction as de
import pandas as pd
import re

class DataCleaning:
    def __init__(self):
        self.dex = de.DataExtractor()
    def standardise_phone_number(self, phone_number):
        phone_str = str(re.sub("([-\s])|(\+..)|(\(.+\))|^(0[0-9]+?\s?)", "", str(phone_number))).lstrip("0")
        return "00" + phone_str
    def clean_user_data(self):
        """ Reads user data from table legacy_users
        """
        user_data = self.dex.read_rds_table("legacy_users")
        print("Read user data")

        """ Cleans country_code to only contain actual country codes 
        with 2 capital letters. Transforms country_code from object to 
        category.
        """
        country_codes = user_data['country_code'].str.fullmatch("([A-Z][A-Z])")
        user_data = user_data[country_codes]
        user_data['country_code'] = user_data['country_code'].astype('category')
        print("Cleaned country codes")

        """ Validates emails with regex
        """
        email_validated = user_data['email_address'].str.fullmatch(".+\@.+\..+")
        user_data = user_data[email_validated]
        print("Cleaned email data")

        """ Corrects and validates phone numbers
        """
        for index, row in user_data.iterrows():
            row = row.copy()
            user_data["phone_number"][index] = self.standardise_phone_number(row["phone_number"])
        #user_data.loc[user_data['phone_number'].str.len() != 10] = pd.NA
        #null_phone_numbers = user_data["phone_number"].isnull()
        #print("Number of null phone numbers:", null_phone_numbers.sum())
        #user_data = user_data.dropna(subset=["phone_number"])

        """ Corrects and validates dates
        """
        user_data["join_date"] = pd.to_datetime(user_data["join_date"], infer_datetime_format=True, errors='coerce') 
        user_data["date_of_birth"] = pd.to_datetime(user_data["date_of_birth"], infer_datetime_format=True, errors='coerce') 
        print("Cleaned dates")

        """ Removes rows containing NULL uuid
        """
        inconsistent_rows = user_data["user_uuid"].isin(["NULL"])
        user_data = user_data[~inconsistent_rows]
        print("Cleaned inconsistencies")

        return user_data

dc = DataCleaning()
print(dc.clean_user_data())