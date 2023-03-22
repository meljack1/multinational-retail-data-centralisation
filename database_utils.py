import data_cleaning as dc
import data_extraction as de
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def read_db_creds(self):
        """ Reads database credentials from db_creds.yaml and returns a dictionary
        """
        with open('db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            return(db_creds)
    def init_db_engine(self):
        """ Creates sqlalchemy engine from credentials returned from read_db_creds()
        """
        db_creds = self.read_db_creds()
        return create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
    def list_db_tables(self):
        """ Lists tables in engine initialised in init_db_engine
        """
        engine = self.init_db_engine()
        engine.connect()
        return inspect(engine).get_table_names()
    def upload_to_db(self, table_name, dataframe):
        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}")
        dataframe.to_sql(table_name, engine, if_exists='replace')

data_cleaner = dc.DataCleaning()
data_extractor = de.DataExtractor()
data_connector = DatabaseConnector()

#data_connector.upload_to_db('dim_users', data_cleaner.clean_user_data(data_extractor, data_connector))
#data_connector.upload_to_db('dim_card_details', data_cleaner.clean_card_data(data_extractor))
#data_connector.upload_to_db('dim_store_details', data_cleaner.clean_store_data(data_extractor))
#data_connector.upload_to_db('dim_products', data_cleaner.clean_products_data(data_extractor))
#data_connector.upload_to_db('orders_table', data_cleaner.clean_orders_data(data_extractor, data_connector))
#data_connector.upload_to_db('dim_date_times', data_cleaner.clean_datetime_data(data_extractor))
#print(data_cleaner.clean_card_data(data_extractor))
