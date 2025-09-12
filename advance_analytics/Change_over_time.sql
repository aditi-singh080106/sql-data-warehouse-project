-- Analyze the sales over time 
SELECT 
order_date ,
sales 
FROM gold_fact_sales 
ORDER BY order_date ;

-- -- Analyze the sales over time  Yearly basis 
SELECT 
YEAR(order_date ) AS order_year ,
SUM(sales) AS total_sales_over_year , 
COUNT(DISTINCT customer_key) AS total_customers , 
SUM(quantity) AS total_quantity
FROM gold_fact_sales 
WHERE order_date IS NOT NULL 
GROUP BY   order_year 
ORDER BY order_year ;             -- A high-level overview insights that helps with strategic decision making 


-- -- Analyze the sales over time  monthly basis 
SELECT 
YEAR(order_date ) AS order_year ,
MONTH(order_date ) AS order_month ,
SUM(sales) AS total_sales_over_year , 
COUNT(DISTINCT customer_key) AS total_customers , 
SUM(quantity) AS total_quantity
FROM gold_fact_sales 
WHERE order_date IS NOT NULL 
GROUP BY  order_year ,  order_month
ORDER BY order_year,  order_month ;             -- Detailed insight to discover seasonality of the data 
