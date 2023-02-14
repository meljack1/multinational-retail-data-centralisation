import data_extraction as de
import pandas as pd

class DataCleaning:
    def __init__(self):
        self.dex = de.DataExtractor()
    def clean_user_data(self):
        """ Reads user data from table legacy_users
        """
        user_data = self.dex.read_rds_table("legacy_users")
        print("Read user data")

        """ Removes rows containing NULL uuid
        """
        inconsistent_rows = user_data["user_uuid"].isin(["NULL"])
        user_data = user_data[~inconsistent_rows]
        print("Cleaned inconsistencies")

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
        #email_validated = user_data['email_address'].str.fullmatch("^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$")
        #user_data = user_data[email_validated]
        #print("Cleaned email data")

        """ Corrects and validates phone numbers
        """
        ##
        
        """ Corrects and validates dates
        """
        user_data["join_date"] = pd.to_datetime(user_data["join_date"], infer_datetime_format=True, errors='coerce') 
        user_data["date_of_birth"] = pd.to_datetime(user_data["date_of_birth"], infer_datetime_format=True, errors='coerce') 
        print("Cleaned dates")

        return user_data["join_date"][1349]

dc = DataCleaning()
print(dc.clean_user_data())