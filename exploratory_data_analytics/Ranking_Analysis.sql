-- Which 5 product generate highest revenue ? 
SELECT 
d.product_name ,
SUM(sales) AS total_revenue 
FROM gold_fact_sales AS f
LEFT JOIN gold_dim_products d 
ON f.product_key = d.product_key 
GROUP BY d.product_name 
ORDER BY total_revenue DESC
LIMIT 5 ;


-- What are the 5 worst - performing product in terms of sales  ? 
SELECT 
d.product_name ,
SUM(sales) AS total_revenue 
FROM gold_fact_sales AS f
LEFT JOIN gold_dim_products d 
ON f.product_key = d.product_key 
GROUP BY d.product_name 
ORDER BY total_revenue 
LIMIT 5 ;


SELECT 
* 
FROM 
(
SELECT 
d.product_name ,
SUM(sales) AS total_revenue ,
ROW_NUMBER() OVER (ORDER BY SUM(sales) DESC ) AS ranking 
FROM gold_fact_sales AS f
LEFT JOIN gold_dim_products d 
ON f.product_key = d.product_key 
GROUP BY d.product_name 
)  t 
WHERE ranking <= 5 ; 


-- Find the top 10 customer who have generated the highest revenue 
SELECT 
d.customer_key ,
d.first_name , 
d.last_name , 
COUNT(DISTINCT f.order_number) AS Number_of_orders
FROM gold_fact_sales AS f 
LEFT JOIN gold_dim_customers d 
ON f.customer_key = d.customer_key 
GROUP BY 
customer_key , 
first_name , 
last_name 
ORDER BY Number_of_orders DESC 
LIMIT 10 ;


-- The Top 3 customer with fewest orders placed 
SELECT 
d.customer_key ,
d.first_name , 
d.last_name , 
COUNT(DISTINCT f.order_number) AS Number_of_orders
FROM gold_fact_sales AS f 
LEFT JOIN gold_dim_customers d 
ON f.customer_key = d.customer_key 
GROUP BY 
customer_key , 
first_name , 
last_name 
ORDER BY Number_of_orders ASC
LIMIT 3 ;
