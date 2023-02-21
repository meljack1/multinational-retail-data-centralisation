#import database_utils as du
import pandas as pd
import tabula

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

de = DataExtractor()

#print(de.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))

#dc = du.DatabaseConnector()
#dc.list_db_tables()