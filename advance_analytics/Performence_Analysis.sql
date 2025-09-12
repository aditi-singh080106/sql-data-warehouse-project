/*
Analyze the yearly performence of the products by comparing there sales to both the average sales 
performence of the product and the previous year's sales 
*/
WITH yearly_product_sales AS  (
SELECT 
YEAR(s.order_date) AS order_year ,
p.product_name , 
SUM(s.sales) AS current_sales   
FROM gold_fact_sales AS s
LEFT JOIN gold_dim_products p 
ON s.product_key = p.product_key 
WHERE order_date IS NOT NULL 
GROUP BY
order_year , 
product_name 
ORDER BY product_name) 
SELECT 
* ,
AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales ,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg_sales 
FROM yearly_product_sales  ;