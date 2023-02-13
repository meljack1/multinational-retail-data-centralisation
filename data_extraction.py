import database_utils as du
import pandas as pd

class DataExtractor:
    def __init__(self):
        self.dc = du.DatabaseConnector()
    def read_rds_table(self, table):
        """ Connects to database and returns a table by name as a pandas dataframe
        """
        engine = self.dc.init_db_engine().connect()
        return pd.read_sql_table(table, engine)

de = DataExtractor()

dc = du.DatabaseConnector()
dc.list_db_tables()