-- Explore all countries our customer came from
SELECT DISTINCT 
country
FROM gold_dim_customers ;

-- Explore all Categories 'The Major Division'
SELECT DISTINCT 
category ,
subcategory , 
product_name
FROM gold_dim_products 
ORDER BY  1,2,3; 

-- SELECT 
-- category , 
-- COUNT(*) AS TOTAL_CATEGORY_COUNT 
-- FROM gold_dim_products
-- GROUP BY category ;