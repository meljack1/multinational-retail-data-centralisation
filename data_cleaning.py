import data_extraction as de
import pandas as pd

class DataCleaning:
    def __init__(self):
        self.dex = de.DataExtractor()
    def clean_user_data(self):
        """ Reads user data from table legacy_users
        """
        user_data = self.dex.read_rds_table("legacy_users")
        """ Removes rows containing NULL uuid
        """
        inconsistent_rows = user_data["user_uuid"].isin(["NULL"])
        user_data = user_data[~inconsistent_rows]
        """ Cleans country_code to only contain actual country codes 
        with 2 capital letters. Transforms country_code from object to 
        category.
        """
        country_codes = user_data['country_code'].str.fullmatch("([A-Z][A-Z])")
        user_data = user_data[country_codes]
        user_data['country_code'] = user_data['country_code'].astype('category')
        return user_data

dc = DataCleaning()
print(dc.clean_user_data())