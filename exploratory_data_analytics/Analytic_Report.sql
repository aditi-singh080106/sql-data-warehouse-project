-- FIND HTE TOTAL CUSTOMER BY COUNTRY 
SELECT country , COUNT(customer_key) AS total_customer FROM gold_dim_customers 
GROUP BY country 
ORDER BY country ;

-- FIND THE TOTAL CUSTOMER BY GENDER 
SELECT
 gender ,
 COUNT(customer_key) AS total_customer 
 FROM gold_dim_customers 
GROUP BY gender
ORDER BY gender ;

-- FIND THE TOTAL PRODUCTS BY CATEGORY 
SELECT
category ,
COUNT(product_key) AS total_product 
FROM gold_dim_products
GROUP BY category 
ORDER BY category ;

-- WHAT IS THE AVERAGE COST IN EACH CATEGORY 
SELECT
category ,
AVG(cost) AS avg_cost
FROM gold_dim_products
GROUP BY category 
ORDER BY category ;

-- WHAT IS THE TOTAL REVENUE GENERATED FOR EACH CATEGORY 
SELECT 
d.category ,
SUM(f.sales) AS total_revenue
FROM gold_fact_sales AS f 
LEFT JOIN gold_dim_products d 
ON f.product_key = d.product_key 
GROUP BY category 
ORDER BY total_revenue DESC ;

-- FIND TOTAL REVENUE GENRATED BY EACH CUSTOMER 
SELECT 
d.customer_key ,
d.first_name ,
d.last_name ,
SUM(f.sales) AS total_revenue 
FROM gold_fact_sales AS f 
LEFT JOIN gold_dim_customers d 
ON f.customer_key = d.customer_key 
GROUP BY 
d.customer_key ,
d.first_name ,
d.last_name
ORDER BY total_revenue  DESC ;

-- WHAT IS THE DISTRIBUTION OF SOLD ITEMS BY COUNTRIES 
SELECT 
d.country AS country ,
SUM(quantity)  AS total_revenue 
FROM gold_fact_sales AS f 
LEFT JOIN gold_dim_customers d 
ON f.customer_key = d.customer_key 
GROUP BY 
d.country
ORDER BY total_revenue  DESC ;


