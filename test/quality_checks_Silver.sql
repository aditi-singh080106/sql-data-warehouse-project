/*
======================================================================================================================================================================
Quality Checks
======================================================================================================================================================================
Script Purpose :
    This script performs various quality checks for data consistency , accuracy , and standardization across  the 'silver' schema . It include checks for :
      - Null or Duplicate primary Key .
      - Unwanted spaces in string fields.
      - Data Standardization and Consistency.
      - Invalid date ranges and orders .
      - Data consistency between related fields .

Using Notes : 
    - Run these checks after loading the data  in Silver Layer .
    - Investigate and resolve any discrepancies found during the checks .
=====================================================================================================================================================================
*/
USE datawarehouse ;

SELECT '=============================================================================================' ;
SELECT '                                 CRM Customer Info Checks                                    ' ;
SELECT '=============================================================================================' ;
-- CHECK THAT THE PRIMARY KEY IS NOT NULL 
-- EXPECTATION : NO RESULT
SELECT 
cst_id ,
COUNT(cst_id) 
FROM silver_crm_cust_info 
WHERE cst_id IS NULL
GROUP BY cst_id;

-- CHECK THAT THE PRIMARY KEY DATA DUPLICACY
-- EXPECTATION : NO RESULT
SELECT
	cst_id,
	COUNT(cst_id)
FROM silver_crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(cst_id) > 1 ;

-- Check for Unwanted spaces 
-- EXPECTATION : No Result 
SELECT 
cst_firstname , 
cst_lastname 
FROM bronze_crm_cust_info 
 WHERE 
cst_firstname != TRIM(cst_firstname)
OR  cst_lastname != TRIM(cst_lastname);

-- Check for Unwanted spaces 
-- EXPECTATION : No Result 
SELECT 
cst_lastname 
FROM silver_crm_cust_info 
WHERE 
cst_lastname != TRIM(cst_lastname);

-- Check for Unwanted spaces 
-- EXPECTATION : No Result 
SELECT 
cst_gndr
FROM silver_crm_cust_info 
WHERE 
cst_gndr != TRIM(cst_gndr);

-- Data Standardization & Consistency 
SELECT 
DISTINCT cst_gndr 
FROM 
silver_crm_cust_info ;

SELECT '=============================================================================================' ;
SELECT '                                 CRM Product Info Checks                                    ' ;
SELECT '=============================================================================================' ;
SELECT * FROM bronze_crm_prod_info ; 

-- Check for duplicate or null values in primary key 
-- Expectation : No Result
SELECT prd_id , COUNT(*) FROM silver_crm_prod_info GROUP BY  prd_id HAVING 	COUNT(*) > 1  OR prd_id  IS NULL;



-- Check for negative or null values
-- EXPECTATION : No Result 

SELECT 
prd_cost ,
COUNT(prd_cost)
FROM
silver_crm_prod_info 
GROUP BY prd_cost
HAVING prd_cost IS NULL ;  


-- CHECK FOR UNWANTED SPACING 
-- EXPECTAION : NO RESULT
SELECT prd_nm FROM 
silver_crm_prod_info 
WHERE prd_nm != TRIM(prd_nm) ;

-- Check for invalid date 
-- EXPECTATION : No Result 
SELECT * FROM silver_crm_prod_info WHERE prd_start_date > prd_end_date ;

SELECT '=============================================================================================' ;
SELECT '                                 CRM Sales Details Checks                                    ' ;
SELECT '=============================================================================================' ;

-- CHECK FOR INVALID DATES
SELECT 
NULLIF(sls_order_dt ,0) AS sls_order_dt
FROM
bronze_crm_sales_detail 
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8;

SELECT 
NULLIF(sls_ship_dt ,0) AS sls_ship_dt
FROM
bronze_crm_sales_detail 
WHERE sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8;

SELECT 
NULLIF(sls_due_dt ,0) AS sls_due_dt
FROM
bronze_crm_sales_detail 
WHERE sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8;

SELECT 
*
FROM bronze_crm_sales_detail
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt ;

-- Check Data Consistency Between : Sales , Price & Quantity
-- >> Sales = Quantity * Price 
-- >> Values must not equal to be Null , Zero  or Negative 
SELECT DISTINCT 
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price )
	THEN sls_sales = sls_quantity * ABS(sls_price)
    ELSE sls_sales 
    END AS sls_sales ,
sls_quantity ,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
	 THEN sls_price = sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price 
     END AS sls_price 
FROM bronze_crm_sales_detail 
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales , sls_quantity , sls_price ;


SELECT '=============================================================================================' ;
SELECT '                                 ERP Customer az12 Checks                                    ' ;
SELECT '=============================================================================================' ;
-- Identify out of range dates
SELECT 
bdate 
FROM 
silver_erp_cust_az12
WHERE bdate > sysdate() ;


-- DATA STANDARDIZATION & NORMALIZATION 
SELECT 
DISTINCT gen 
FROM 
silver_erp_cust_az12 ;

SELECT * FROM silver_erp_cust_az12 ;

SELECT '=============================================================================================' ;
SELECT '                                 ERP Customer a101 Checks                                    ' ;
SELECT '=============================================================================================' ;

SELECT cst_key FROM silver_crm_cust_info ;

-- Data Standardization and Consistency  
SELECT DISTINCT 
CASE WHEN TRIM(cntry) IN ('US','USA') THEN  'United States'
	WHEN  TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) = ''  OR cntry IS NULL THEN 'N/A'
    ELSE TRIM(cntry)
END AS cntry 
FROM silver_erp_loc_a101 ;

SELECT '=============================================================================================' ;
SELECT '                                 ERP Product Category Checks                                 ' ;
SELECT '=============================================================================================' ;

SELECT * FROM silver_erp_px_cat_g1v2 ;
-- Check For Unwanted Space 
SELECT 
*
FROM bronze_erp_px_cat_g1v2 
WHERE cat != TRIM(CAT)  OR SUBCAT != TRIM(SUBCAT) OR maintenance != TRIM(maintenance);

-- DATA STANDARDIZATION & CONSISTENCY
SELECT 
DISTINCT subcat 
FROM bronze_erp_px_cat_g1v2  
WHERE subcat !=  TRIM(subcat);

SELECT 
DISTINCT maintenance
FROM bronze_erp_px_cat_g1v2  
WHERE maintenance !=  TRIM(maintenance);

SELECT * FROM silver_crm_prod_info ;
