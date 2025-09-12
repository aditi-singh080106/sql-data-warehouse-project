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
    order_year,
    product_name,
    current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales ,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg_sales ,
CASE WHEN (current_sales - AVG(current_sales) OVER (PARTITION BY product_name)) > 0 THEN 'Above Average' 
     WHEN (current_sales - AVG(current_sales) OVER (PARTITION BY product_name)) < 0 THEN 'Below Average' 
     ELSE 'Average' 
     END AS performace ,
-- -- YEAR OVER YEAR ANALYSIS 
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales , 
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_diff ,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increse' 
	 WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease' 
     ELSE 'No Change' 
     END AS previous_year_change
FROM yearly_product_sales  ;



-- -- MONTH - OVER- MONTH ANALYSIS 
WITH monthly_product_sales AS  (
SELECT 
MONTH(s.order_date) AS order_month ,
p.product_name , 
SUM(s.sales) AS current_sales   
FROM gold_fact_sales AS s
LEFT JOIN gold_dim_products p 
ON s.product_key = p.product_key 
WHERE order_date IS NOT NULL 
GROUP BY
order_month , 
product_name 
ORDER BY product_name) 
SELECT 
    order_month,
    product_name,
    current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales ,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg_sales ,
CASE WHEN (current_sales - AVG(current_sales) OVER (PARTITION BY product_name)) > 0 THEN 'Above Average' 
     WHEN (current_sales - AVG(current_sales) OVER (PARTITION BY product_name)) < 0 THEN 'Below Average' 
     ELSE 'Average' 
     END AS performace ,
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) AS previous_month_sales , 
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) AS previous_month_diff ,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) > 0 THEN 'Increse' 
	 WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) < 0 THEN 'Decrease' 
     ELSE 'No Change' 
     END AS previous_month_change
FROM monthly_product_sales  ;
