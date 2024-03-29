#import database_utils as du
import pandas as pd
import tabula
import requests
import boto3

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
        df = pd.DataFrame(data).transpose()
        return df
    def extract_from_s3(self, url):
        s3 = boto3.client('s3')
        url_parts = url.split("/")
        s3.download_file(url_parts[2], url_parts[3], url_parts[3])
        if url_parts[3].split(".")[1] == "json":
            return pd.read_json(url_parts[3])
        elif url_parts[3].split(".")[1] == "csv":
            return pd.read_csv(url_parts[3])
        return NameError("No json or csv file found.")

de = DataExtractor()
