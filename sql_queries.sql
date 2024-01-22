/* Cast the columns of the orders_table to the correct data types. */
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19);
ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::VARCHAR(12);

/* Cast the columns of the dim_users_table to the correct data types */
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::VARCHAR(255);
ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::VARCHAR(255);
ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE;
ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::VARCHAR(2);
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

/* Update the dim_store_details table to only have one latitude column and to have the correct data types */
UPDATE dim_store_details  
SET latitude = REPLACE(latitude,latitude, CONCAT(latitude, lat));

UPDATE dim_store_details
/* Set lat and long to null if store is an online store */
SET latitude = null, longitude = null
WHERE store_code = 'WEB-1388012W'

ALTER TABLE dim_store_details
DROP COLUMN lat;
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT;
ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255) USING locality::VARCHAR(255);
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::VARCHAR(12);
ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;
ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE;
ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::VARCHAR(255);
ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT;
ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::VARCHAR(2);
ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255) USING continent::VARCHAR(255);

/* Update dim_products to remove £ sign from price, and sort products by weight category */
UPDATE dim_products SET product_price = REPLACE(product_price, '£', '')

ALTER TABLE dim_products 
ADD COLUMN weight_class TEXT; 

UPDATE dim_products 
SET weight_class = CASE 
	   		WHEN weight < 2 THEN 'Light'
            WHEN weight < 41 THEN 'Mid_Sized'
			WHEN weight < 141 THEN 'Heavy'
            ELSE 'Truck_Required'
END; 

/* Update the dim_products table with the required data types */ 
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;
ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;
ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(13) USING "EAN"::VARCHAR(13);
ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11);
ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;
ALTER TABLE dim_products
ALTER COLUMN "uuid" TYPE UUID USING "uuid"::UUID;
ALTER TABLE dim_products
ALTER COLUMN removed TYPE BOOL USING CASE removed WHEN 'Still_available' THEN TRUE ELSE FALSE END;
ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14) USING weight_class::VARCHAR(14);

/* Update the dim_date_times table with correct data types */ 
ALTER TABLE dim_date_times
ALTER COLUMN "month" TYPE CHAR(2) USING "month"::CHAR(2);
ALTER TABLE dim_date_times
ALTER COLUMN "year" TYPE CHAR(4) USING "year"::CHAR(4);
ALTER TABLE dim_date_times
ALTER COLUMN "day" TYPE CHAR(2) USING "day"::CHAR(2);
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10) USING time_period::VARCHAR(10);
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

/* Update the dim_card_details table with correct types */ 
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19);
ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(5) USING expiry_date::VARCHAR(5);
ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

/* Create primary keys in the dimension tables */
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

/* Finalise the star-based schema and add foreign keys to the orders table */
ALTER TABLE orders_table
ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
ALTER TABLE orders_table
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
ALTER TABLE orders_table
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
ALTER TABLE orders_table
ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
