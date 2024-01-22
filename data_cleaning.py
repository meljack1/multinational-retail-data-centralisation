import data_extraction as de
import pandas as pd
import re

class DataCleaning:
    def standardise_phone_number(self, phone_number):
        phone_str = str(re.sub("([-\s])|(\+..)|(\()|(\))", "", str(phone_number))).lstrip("0")
        return "00" + phone_str
    def clean_user_data(self, dex, du):
        """ Reads user data from table legacy_users
        """
        user_data = dex.read_rds_table("legacy_users", du)
        print("Read user data in user data")

        """ Cleans country_code to only contain actual country codes 
        with 2 capital letters. Transforms country_code from object to 
        category.
        """
        user_data['country_code'] = user_data['country_code'].str.slice(start=-2)
        user_data['country_code'] = user_data['country_code'].astype('category')
        print("Cleaned country codes in user data")

        """ Validates emails with regex
        """
        email_validated = user_data['email_address'].str.fullmatch(".+\@.+\..+")
        user_data = user_data[email_validated]
        print("Cleaned email data in user data")

        """ Corrects and validates phone numbers
        """
        for index, row in user_data.iterrows():
            row = row.copy()
            user_data["phone_number"][index] = self.standardise_phone_number(row["phone_number"])

        """ Corrects and validates dates
        """
        user_data["join_date"] = pd.to_datetime(user_data["join_date"], infer_datetime_format=True, errors='coerce') 
        user_data["date_of_birth"] = pd.to_datetime(user_data["date_of_birth"], infer_datetime_format=True, errors='coerce') 
        print("Cleaned dates in user data")

        """ Removes rows containing NULL uuid
        """
        inconsistent_rows = user_data["user_uuid"].isin(["NULL"])
        user_data = user_data[~inconsistent_rows]
        print("Cleaned inconsistencies in user data")

        return user_data
    def clean_card_data(self, dex):
        card_data = dex.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        
        """ Removes trailing question marks.
        """
        card_data["card_number"] = card_data["card_number"].str.strip("?")
        print("Removed trailing question marks in card data")

        """ Removes rows containing NULL or NaN card_number
        """
        inconsistent_rows = card_data["card_number"].isin(["NULL"])
        card_data = card_data[~inconsistent_rows]

        nan_rows = card_data["card_number"].isnull()
        card_data = card_data[~nan_rows]
        print("Cleaned inconsistencies in card data")

        """ Removes non-numerical card_numbers
        """
        card_number_validated = card_data['card_number'].str.fullmatch("[0-9]+")
        card_data = card_data[card_number_validated]
        print("Removed non-numerical card numbers in card data")

        return card_data
    def clean_store_data(self, dex):
        header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        store_data = dex.retrieve_stores_data(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/", header)

        """ Corrects and validates dates
        """
        store_data["opening_date"] = pd.to_datetime(store_data["opening_date"], infer_datetime_format=True, errors='coerce') 
        print("Cleaned dates in store data")

        """ Cleans country_code to only contain actual country codes 
        with 2 capital letters. Transforms country_code from object to 
        category.
        """
        country_codes = store_data['country_code'].str.fullmatch("([A-Z][A-Z])")
        store_data = store_data[country_codes]
        store_data['country_code'] = store_data['country_code'].astype('category')
        print("Cleaned country codes in store data")

        """ Removes non-numerical staff numbers
        """
        store_data['staff_numbers'] = store_data['staff_numbers'].str.replace('[^0-9]','', regex=True)
        print("Removed non-numerical staff numbers in store data")

        """ Removes rows containing NULL or NaN latitude
        """
        inconsistent_rows = store_data["store_code"].isin(["NULL"])
        store_data = store_data[~inconsistent_rows]

        nan_rows = store_data["store_code"].isnull()
        store_data = store_data[~nan_rows]
        print("Cleaned inconsistencies in store data")

        return store_data
    def convert_product_weights(self, weight):
        if not isinstance(weight, str):
            return
        elif re.match("(([0-9]+\s+x\s+)?[0-9]+.?[0-9]*kg)", weight):
            return '{0:.2f}'.format(float(eval(''.join(c for c in weight.replace("x", "*") if c in "0123456789.*"))))
        elif re.match("(([0-9]+\s+x\s+)?[0-9]+.?[0-9]*(g|ml))", weight):
            return '{0:.2f}'.format(float(eval(''.join(c for c in weight.replace("x", "*") if c in "0123456789.*")))/1000)
        elif re.match("([0-9]+.?[0-9]*oz)", weight):
            return '{0:.2f}'.format(float(''.join(c for c in weight if c in "0123456789.*")) * 0.0283495)
        else:
            print(weight)
            return
    def clean_products_data(self, dex):
        product_data = dex.extract_from_s3("s3://data-handling-public/products.csv")

        """ Converts product weight to kg
        """
        product_data["weight"] = product_data["weight"].apply(self.convert_product_weights)

        """ Removes rows containing NULL or NaN weight
        """
        inconsistent_rows = product_data["weight"].isin(["NULL"])
        product_data = product_data[~inconsistent_rows]

        nan_rows = product_data["weight"].isnull()
        product_data = product_data[~nan_rows]

        print("Cleaned inconsistencies in product data")

        """ Removes non-numerical weights
        """
        product_data_validated = product_data['weight'].str.fullmatch("[0-9.]+")
        product_data = product_data[product_data_validated]
        print("Removed non-numerical weights in product data")

        return product_data
    def clean_orders_data(self, dex, du):
        orders_data = dex.read_rds_table("orders_table", du)
        orders_data = orders_data.drop(columns=["first_name", "last_name", "1"])
        return orders_data
    def clean_datetime_data(self, dex):
        datetime_data = dex.extract_from_s3("s3://data-handling-public/date_details.json")

        """ Removes rows containing NULL or NaN date_uuid
        """
        inconsistent_rows = datetime_data["date_uuid"].isin(["NULL"])
        datetime_data = datetime_data[~inconsistent_rows]

        nan_rows = datetime_data["date_uuid"].isnull()
        datetime_data = datetime_data[~nan_rows]

        """ Removes non-numerical months
        """
        datetime_data_validated = datetime_data['month'].str.fullmatch("[0-9]+")
        datetime_data = datetime_data[datetime_data_validated]
        print("Removed non-numerical weights in datetime data")

        return datetime_data
 