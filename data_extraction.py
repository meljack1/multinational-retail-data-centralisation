#import database_utils as du
import pandas as pd

class DataExtractor:
    def read_rds_table(self, table, du):
        """ Connects to database and returns a table by name as a pandas dataframe
        """
        engine = du.init_db_engine().connect()
        return pd.read_sql_table(table, engine)

#de = DataExtractor()

#dc = du.DatabaseConnector()
#dc.list_db_tables()