import yaml
from sqlalchemy import create_engine


class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            return(db_creds)
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return engine

dbc = DatabaseConnector()
print(dbc.init_db_engine())