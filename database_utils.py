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
 