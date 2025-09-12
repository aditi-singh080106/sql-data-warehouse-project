-- Find the total Sales
SELECT SUM(sales) AS total_sales FROM gold_fact_sales ;

-- Fint how many items are sold 
SELECT SUM(quantity) AS total_quantity FROM gold_fact_sales ;

-- Find avreage selling price  
SELECT AVG(price) AS avg_price FROM gold_fact_sales ;

-- Find total number of orders
SELECT COUNT(order_number) As total_oders FROM gold_fact_sales ; 
SELECT COUNT(DISTINCT order_number) As total_oders FROM gold_fact_sales ; 

-- Find the total number of product 
SELECT COUNT(PRODUCT_KEY) AS total_product FROM  gold_dim_products ;
SELECT COUNT( DISTINCT PRODUCT_KEY) AS total_product FROM  gold_dim_products ;


-- Find the total number of customer 
SELECT COUNT(CUSTOMER_ID) FROM gold_dim_customers ;

-- Find the total number of customer who has placed an order
SELECT COUNT(DISTINCT customer_key) FROM gold_fact_sales ;
SELECT COUNT(DISTINCT order_number) FROM gold_fact_sales ;
SELECT COUNT(order_number) FROM gold_fact_sales ;
SELECT COUNT(CUSTOMER_key) FROM gold_fact_sales ;



-- ANALYSIS TABLE
-- Generate report that show all key metrics of the business 
SELECT 'Total Sales' AS measure_name  , SUM(sales) AS measure_value FROM gold_fact_sales 
UNION ALL 
SELECT 'Total Quantity' ,  SUM(quantity)  FROM  gold_fact_sales 
UNION ALL 
SELECT 'Average Price' ,  AVG(price)  FROM  gold_fact_sales 
UNION ALL 
SELECT 'Total Number Of Orders' ,  COUNT(DISTINCT order_number)  FROM  gold_fact_sales 
UNION ALL 
SELECT 'Total Number of Products' ,  COUNT( DISTINCT PRODUCT_KEY)  FROM  gold_dim_products 
UNION ALL 
SELECT 'Total Number of Customer ' , COUNT(DISTINCT customer_key) FROM gold_fact_sales ;
