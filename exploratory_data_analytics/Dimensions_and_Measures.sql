SELECT DISTINCT 
* 
FROM gold_dim_products ;      

SELECT DISTINCT 
category
FROM gold_dim_products ;      -- Dimension

SELECT DISTINCT 
product_name
FROM gold_dim_products ;      -- Dimension

SELECT 
sales						 -- numeric value , can aggregated : Measure
FROM gold_fact_sales ;

SELECT DISTINCT
quantity						 -- numeric value , can aggregated : Measure
FROM gold_fact_sales ;

SELECT 
*			 
FROM gold_fact_sales ;

SELECT 
* ,
CASE WHEN BIRTHDATE < sysdate() 
		THEN (YEAR(SYSDATE() )-YEAR(BIRTHDATE) )
	ELSE BIRTHDATE 
    END AS age                     -- Since we can calculate the age from birthdat then birthdate is : Measure
FROM gold_dim_customers ;

SELECT 
customer_id                        -- It's a numeric column but it's aggregation would not helpful further so it is : Dimension
FROM gold_dim_customers ;