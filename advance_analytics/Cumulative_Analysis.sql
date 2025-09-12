-- Calculate the total sales per month 
-- and the runnig total of sales over-time 
SELECT 
order_year ,
order_month , 
total_sales ,
SUM(total_sales) OVER (PARTITION BY order_year ORDER BY order_month) AS running_total_sales
FROM 
(
SELECT 
YEAR(order_date) AS order_year ,
MONTH(order_date) AS order_month ,
SUM(sales) AS total_sales 
FROM gold_fact_sales 
WHERE order_date IS NOT NULL 
GROUP BY  
order_year , 
order_month 
ORDER BY 
order_year , 
order_month) t
 ;