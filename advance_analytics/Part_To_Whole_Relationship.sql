-- Which category contribute most in over all sales ? 
WITH  category_sales AS (
SELECT 
p.category , 
SUM(sales) AS total_sales 
FROM gold_fact_sales AS s 
LEFT JOIN gold_dim_products AS p 
ON s.product_key = p.product_key 
GROUP BY category 
ORDER BY category 
)
SELECT 
category , 
total_sales , 
SUM(total_sales) OVER ()  AS over_all_sales , 
CONCAT(ROUND((total_sales /SUM(total_sales) OVER () ) * 100, 2) , '%') AS sales_percentage 
FROM category_sales
ORDER BY total_sales DESC ;