-- Find the date of first and last order 
SELECT 
MIN(order_date)  AS first_order_date ,
MAX(order_date) AS last_order_date ,
YEAR(MAX(order_date)) - YEAR(MIN(order_date))AS order_range_year
FROM gold_fact_sales ;


-- Find the youngest and oldest date 
SELECT 
MIN(birthdate) AS youngest_birthdate , 
YEAR(sysdate())-YEAR(MIN(birthdate)) AS oldest_age ,
MAX(birthdate) AS oldest_birthdate ,
YEAR(sysdate())-YEAR(MAX(birthdate))  AS younest_age
FROM 
gold_dim_customers ; 