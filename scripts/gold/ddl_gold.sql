/*
======================================================================================================================================================================
DDL Script : Create Gold Views
======================================================================================================================================================================
Script Purpose : 
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension amd fact tables ( Star Schema )

    Each view performs tranformations and combines data from the Silver layer o produce a clean , enriched , and  business-ready dataset.

Usage : 
      - These view can be queried foe analytics and repoting .
=====================================================================================================================================================================
*/

-- =================================================================================================================================================================
-- Create Dimension : gold_dim_customers
-- =================================================================================================================================================================
SELECT * FROM gold_dim_customers ;
SELECT DISTINCT gender  FROM gold_dim_customers ;

CREATE VIEW gold_dim_customers AS 
SELECT 
ROW_NUMBER () OVER (ORDER BY cst_id) AS customer_key ,
	ci.cst_id  AS customer_id,
	ci.cst_key AS customer_number, 
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
    CASE WHEN cst_gndr != 'N/A' THEN cst_gndr 
	ELSE COALESCE(ca.gen,'N/A') 
END AS gender , 					-- CRM is master table for gender data integration 
	ci.cst_marital_status AS marital_status ,
    la.cntry AS country , 
	ci.cst_create_date  AS create_date, 
	ca.bdate AS birthdate
FROM silver_crm_cust_info  AS ci
LEFT JOIN silver_erp_cust_az12 AS ca 
ON 		ci.cst_key = ca.cid 
LEFT JOIN silver_erp_loc_a101 AS la 
ON 		ci.cst_key = la.cid ;

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
-- Create Dimension : gold_dim_products
-- =================================================================================================================================================================
SELECT * FROM gold_dim_customers ;

CREATE VIEW gold_dim_products AS 
SELECT 
ROW_NUMBER() OVER ( ORDER BY prd_key , prd_start_date) AS product_key , -- sarrogate primary key 
pn.prd_id AS product_id ,
pn.prd_key AS product_number ,
pn.prd_nm As product_name ,
pn.cat_id AS Category_id ,
pc.cat AS category ,
pc.subcat AS subcategory ,
pc.maintenance ,
pn.prd_cost As cost ,
pn.prd_line AS product_line ,
pn.prd_start_date AS start_date 
-- pn.prd_end_date  
FROM silver_crm_prod_info AS pn 
LEFT JOIN silver_erp_px_cat_g1v2 AS pc 
ON pn.cat_id = pc.id
WHERE prd_end_date IS NULL  ; -- Filter out all historical data


SELECT prd_key , COUNT(*) FROM 
(
SELECT 
pn.prd_id ,
pn.cat_id ,
pn.prd_key ,
pn.prd_nm ,
pn.prd_cost ,
pn.prd_line ,
pn.prd_start_date ,
pn.prd_end_date  ,
pc.cat ,
pc.subcat ,
pc.maintenance
FROM silver_crm_prod_info AS pn 
LEFT JOIN silver_erp_px_cat_g1v2 AS pc 
ON pn.cat_id = pc.id
WHERE prd_end_date IS NULL -- Filter out all historical data ; 
)T 
GROUP BY prd_key 
HAVING COUNT(*) > 1 ;

-- =================================================================================================================================================================
-- Create Dimension : gold_fact_sales
-- =================================================================================================================================================================
USE datawarehouse ;
CREATE VIEW gold_fact_sales AS 
SELECT 
sd.sls_ord_num AS order_number,
pd.product_key,
cs.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS ship_date,
sd.sls_due_dt AS due_date, 
sd.sls_sales AS sales , 
sd.sls_quantity AS quantity , 
sd.sls_price  AS price
FROM 
silver_crm_sales_detail sd 
LEFT JOIN gold_dim_products pd
ON sd.sls_prd_key = pd.product_number    -- USE THE DIMENSION'S SURROGATE KEY INSTEAD OF ID'S TO EASILY CONNECT FATS WITH DIMENSIONS
LEFT JOIN gold_dim_customers cs 
ON sd.sls_cust_id = cs.customer_id ;  

-- FOREIGN KEY (Integrity) 
SELECT 
* 
FROM gold_fact_sales AS f
LEFT JOIN gold_dim_products AS dp
ON f.product_key = dp.product_key
LEFT JOIN gold_dim_customers AS dc 
ON f.customer_key = dc.customer_key 
WHERE dp.product_key IS NULL  OR dc.customer_key IS NULL ;
