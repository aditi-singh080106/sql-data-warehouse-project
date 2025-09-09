/*
====================================================================================================================================================================
Quality Checks :
====================================================================================================================================================================
Script Purpose : 
    This script performs quality checks to validate the integrity , consistency , and accuracy of the Gold layer . These  checks ensure : 
    - Uniqueness of surrogate keys in dimension tables.
    - Refrential integrity between fact and dimension tables.
    - Vaildation of relationship in the data model for analytical purposes.

Usage Notes :
    - Run these checks after data loading Silver layer .
    - Investigate and resolve any discrepancies found during the checks .
=====================================================================================================================================================================
*/

-- ==================================================================================================================================================================
-- Checking 'gold_dim_customers'
-- ==================================================================================================================================================================
-- Check for Uniqueness of customer key in gold_dim_customers
-- Expectation : No result 

SELECT 
cst_key ,
COUNT(*) AS duplicate_key 
from silver_crm_cust_info 
GROUP BY cst_key
HAVING COUNT(*) >1 ;


-- data integration 
SELECT 
DISTINCT
ci.cst_gndr , 
ca.gen ,
CASE WHEN cst_gndr != 'N/A' THEN cst_gndr 
	ELSE COALESCE(ca.gen,'N/A') 
END AS new_gen
FROM silver_crm_cust_info  AS ci
LEFT JOIN silver_erp_cust_az12 AS ca 
ON 		ci.cst_key = ca.cid ;

-- =================================================================================================================================================================
-- Checking 'gold.product_key'
-- ==================================================================================================================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  ;
