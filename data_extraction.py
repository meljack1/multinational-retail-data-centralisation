#import database_utils as du
import pandas as pd
import tabula
import requests

header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

class DataExtractor:
    def read_rds_table(self, table, du):
        """ Connects to database and returns a table by name as a pandas dataframe
        """
        engine = du.init_db_engine().connect()
        return pd.read_sql_table(table, engine)
    def retrieve_pdf_data(self, link):
        tabula.convert_into(link, "data.csv", output_format="csv", pages='all')
        df = pd.read_csv('data.csv')
        return df
    def list_number_of_stores(self, endpoint, header):
        number_of_stores = requests.get(endpoint, headers=header)
        return number_of_stores
    def retrieve_stores_data(self, endpoint, header):
        number_stores = int(self.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header).json()["number_stores"])
        print(number_stores)
        data = {}
        for number in range(number_stores):
            data[number] = requests.get(endpoint + str(number), headers=header).json()
        df = pd.DataFrame(data)
        return df

de = DataExtractor()

print(de.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header).text)
print(de.retrieve_stores_data(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/", header))

#print(de.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))

#dc = du.DatabaseConnector()
#dc.list_db_tables()