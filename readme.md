# Multinational Retail Data Centralisation

## Overview
This project creates a local PostgreSQL database in which extract, clean and transform data from a variety of sources, preparing the data for modelling within a star-based database schema.

## Installation Instructions
1. Clone repo to local machine
```
git clone https://github.com/meljack1/multinational-retail-data-centralisation.git
```
2. In pgadmin4, [initialise a database](https://www.pgadmin.org/docs/pgadmin4/development/database_dialog.html) locally called ```sales_data``` to store extracted data. 

3. Create your db_creds.yaml file based on [db_creds_EXAMPLE.yaml](./db_creds_EXAMPLE.yaml), using your login details for the appropriate AWS relational database as well as your local pgadmin4 ```sales_data``` database.

## Usage Instructions 
The data in this project is pulled from multiple sources:

- ```legacy_users``` table - user data - AiCore AWS RDS Postgres database - retrieved by connecting to the database via the DatabaseConnector utility
- ```order_table``` table - order data - AiCore AWS RDS Postgres database - retrieved by connecting to the database via the DatabaseConnector utility
- ```card_details.pdf``` - credit/debit card details - table in a PDF file - tabula used to convert PDF to a .csv file to be read using pandas
- ```store_details``` table - store details - RESTful API - data received in JSON format via API and transposed to a dataframe with pandas
- ```products.csv``` - product details - .csv file stored in S3 bucket - data retrieved with boto3 and read by pandas
- ```date_details.json``` - datetime details - JSON file stored in S3 bucket - data retrieved with boto3 and read by pandas

1. Run [database_utils.py](./database_utils.py) in the terminal to transform the data.
2. Run the queries in [sql_queries.sql](./sql_queries.sql) to create the star-based schema.
3. [data_analysis.sql](./data_analysis.sql) contains some examples of queries on the completed and cleaned database to give useful information.
