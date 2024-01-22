/* How many stores does the business have and in which countries? */
SELECT country_code, COUNT(*) 
FROM dim_store_details 
GROUP BY country_code 
ORDER BY 2 DESC /* SECOND COLUMN */

/* Which locations currently have the most stores? */ 
SELECT locality, COUNT(*) 
FROM dim_store_details 
GROUP BY locality 
ORDER BY 2 DESC /* SECOND COLUMN */

/* Which months produced the largest amount of sales? */
SELECT 
	ROUND(CAST(SUM(product_quantity * product_price) as numeric), 2) as total_sales,
	"month"
FROM orders_table
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code 
INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid 
GROUP BY "month"
ORDER BY 1 DESC

/* How many sales are coming from online? */ 
SELECT 
	COUNT(product_quantity) as numbers_of_sales,
	SUM(product_quantity) as product_quantity_count,
	CASE 
		WHEN store_type = 'Web Portal' 
		THEN 'Web' 
		ELSE 'Offline' 
	END as "location" 
FROM orders_table
INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code 
GROUP BY "location"

/* What percentage of sales come through each type of store? */ 
SELECT 
	store_type,
	ROUND(CAST(SUM(product_quantity * product_price) as numeric), 2) as total_sales,
	ROUND((COUNT(*) / CAST( SUM(count(*)) over () as numeric) * 100), 2) as "percentage_total(%)"
FROM orders_table
INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code 
GROUP BY store_type
ORDER BY "percentage_total(%)" DESC

/* Which month in each year produced the highest cost of sales? */ 
SELECT
	ROUND(CAST(SUM(product_quantity * product_price) as numeric), 2) as total_sales,
	"year",
	"month"
FROM orders_table
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code 
INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid 
GROUP BY "year", "month"
ORDER BY 1 DESC
LIMIT 10

/* What is our staff headcount? */ 
SELECT
	ROUND(CAST(SUM(product_quantity * product_price) as numeric), 2) as total_sales,
	store_type,
	country_code
FROM orders_table
INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code 
GROUP BY store_type, country_code

/* Which German store type is selling the most? */

SELECT
	ROUND(CAST(COUNT(*) as numeric), 2) as total_sales,
	store_type,
	country_code
FROM orders_table
LEFT JOIN dim_products ON dim_products.product_code = orders_table.product_code 
LEFT JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
GROUP BY store_type, country_code
HAVING country_code = 'DE'
ORDER BY 1 DESC

/* How quickly is the company making sales? */ 
WITH cte AS (
	SELECT 
		TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') as date_times,
		year
	FROM dim_date_times
	ORDER BY date_times DESC
), 
cte2 AS (
	SELECT
		year,
		date_times,
		LEAD(date_times, 1) OVER (
			ORDER BY date_times DESC
		) as time_difference
	FROM cte
)

SELECT 
	year,
	AVG(date_times - time_difference) as actual_time_taken
FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC;