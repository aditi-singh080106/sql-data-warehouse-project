/*
===================================================================================================================================================================
Stored Procedure : Load Silver layer ( Bronze -> Silver ) 
===================================================================================================================================================================
Script Purpose : 
    This stored prcedure performs the ETL (Extract , Transform , Load ) procees to populate the 'silver' schema tables from the 'bronze' schema  .
Action Performed :
    - Truncate Silver Tables .
    - Inser5t transformed and cleansed data from Bronze into Silver Tables .

Parameter : 
    - None 
    - This stored procedure does not accept any parameter or return any values .

Usage Example : 
    CALL load_SILVER_layer_data();
====================================================================================================================================================================
*/
USE datawarehouse ;
CALL load_SILVER_layer_data();

DELIMITER // 
CREATE  PROCEDURE load_silver_layer_data()
BEGIN
	DECLARE srt_time DATETIME ;
    DECLARE end_time DATETIME ;
    DECLARE batch_srt_time DATETIME ;
    DECLARE batch_end_time DATETIME ;
	SELECT '==================================================================================================================================' ;
	SELECT '                                      LOADING SILVER LAYER                                                                        ';
	SELECT '==================================================================================================================================' ;

	SET batch_srt_time = sysdate();
	SELECT '==================================================================================================================================' ;
	SELECT '                                      LOADING CRM TABLES                                                                          ';
	SELECT '==================================================================================================================================' ;    
    
	SELECT '>>Truncating Table: silver_crm_cust_info ' ;
	TRUNCATE TABLE silver_crm_cust_info ;
	SELECT 'Inserting Data into : silver_crm_cust_info ' ;
    SET srt_time = sysdate() ;
	INSERT INTO silver_crm_cust_info(
		cst_id ,
		cst_key ,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT 
	cst_id ,
	cst_key ,
	TRIM(cst_firstname) AS  cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'N/A' -- Normalize the marital_status data in a readable format
	END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'N/A'  -- Normalize the gender data in a readable format
	END AS cst_gndr,
	cst_create_date
	FROM(
	SELECT 
	* ,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze_crm_cust_info
	)t WHERE flag_last = 1 ;  -- Select the most recent record per customer
        SET end_time = sysdate();
     SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
	 SELECT count( *) from silver_crm_cust_info ;


	SELECT '>>Truncating Table: silver_crm_prod_info ' ;
	TRUNCATE TABLE silver_crm_prod_info ;
	SELECT 'Inserting Data into : silver_crm_prod_info ' ;
	SET srt_time = sysdate();
	INSERT INTO silver_crm_prod_info (
	  prd_id ,
	  cat_id ,   
	  prd_key ,
	  prd_nm ,
	  prd_cost ,
	  prd_line ,
	  prd_start_date,
	  prd_end_date
	)
	SELECT   
	  prd_id ,   
	  REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Extract category id
	  SUBSTRING(prd_key,7,LENGTH(PRD_KEY)) AS prd_key ,  -- Extract product key
	  prd_nm ,   
	  prd_cost ,   
	  CASE  UPPER(TRIM(prd_line))        -- Map product line code into diescriptive values
		  WHEN 'M' THEN 'Mountain'        
		  WHEN 'R' THEN 'Road'        
		  WHEN 'S' THEN 'Other Sales'       
		  WHEN 'T' THEN 'Touring'       
		  ELSE 'N/A'    
	  END AS prd_line ,   
	CASE 
		  WHEN prd_start_date REGEXP '^[0-9]{8}$'      -- Handle mixed string/DATE format
			   THEN STR_TO_DATE(prd_start_date, '%Y%m%d')
		  ELSE CAST(prd_start_date AS DATE)
	  END AS prd_start_date,
	DATE_SUB(
		  CASE 
			  WHEN LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) REGEXP '^[0-9]{8}$'
				   THEN STR_TO_DATE(LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date), '%Y%m%d')
			  ELSE CAST(LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) AS DATE)
		  END,
		  INTERVAL 1 DAY
	  ) AS prd_end_date -- Calculate date as 1 day before the next  start date 

	FROM bronze_crm_prod_info;
	SET end_time = sysdate();
	SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
	SELECT COUNT(*) FROM silver_crm_prod_info;


	SELECT '>>Truncating Table: silver_crm_sales_detail ' ;
	TRUNCATE TABLE silver_crm_sales_detail ;
	SELECT 'Inserting Data into : silver_crm_sales_detail ' ;
	set srt_time = sysdate();
	INSERT INTO silver_crm_sales_detail(
	sls_ord_num ,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt ,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	CASE WHEN LENGTH(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL 
		ELSE CAST(CAST(sls_order_dt AS CHAR(50)) AS DATE)
		END AS sls_order_dt,
	CASE WHEN LENGTH(sls_ship_dt) != 8 OR sls_order_dt = 0 THEN NULL 
		 ELSE CAST(CAST(sls_ship_dt AS CHAR(50)) AS DATE)
		 END AS sls_ship_dt,
	CASE WHEN LENGTH(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL 
		ELSE CAST(CAST(sls_due_dt AS CHAR(50)) AS DATE)
		END AS sls_due_dt ,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price )
		THEN  sls_quantity * ABS(sls_price)
		ELSE sls_sales 
		END AS sls_sales ,
	sls_quantity ,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
		 THEN sls_price = sls_sales / NULLIF(sls_quantity,0)
		 ELSE sls_price 
		 END AS sls_price 
	FROM bronze_crm_sales_detail 
	WHERE sls_prd_key IN (SELECT prd_key FROM silver_crm_prod_info);
	set end_time = sysdate();
	SELECT COUNT(*) FROM silver_crm_sales_detail;
    SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
   
    SELECT '==================================================================================================================================' ;
	SELECT '                                      LOADING ERP TABLES                                                                          ';
	SELECT '==================================================================================================================================' ;


	SELECT '>>Truncating Table: silver_erp_cust_az12 ' ;
	TRUNCATE TABLE silver_erp_cust_az12 ;
	SELECT 'Inserting Data into : silver_erp_cust_az12 ' ;
	set srt_time = sysdate();
	INSERT INTO silver_erp_cust_az12(
	cid,
	bdate,
	gen
	)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
		ELSE cid 
		END AS cid ,          -- Remove 'NAS" prefix from cid
	CASE WHEN bdate > SYSDATE() THEN NULL 
		ELSE bdate 
	END AS bdate ,            -- Set future dates to NULL 
	CASE WHEN UPPER(TRIM(gen)) IN  ('F' , 'FEMALE' ) THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN  ('M' , 'MALE' ) THEN 'Male'
		ELSE 'N/A'
	END AS gen              -- Normalize the gender values and handel unkonwn cases
	FROM 
	bronze_erp_cust_az12 ;
    
	 set end_time = sysdate();
	SELECT COUNT(*) FROM silver_erp_cust_az12;
    SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');


	SELECT '>>Truncating Table: silver_erp_loc_a101 ' ;
	TRUNCATE TABLE silver_erp_loc_a101 ;
	SELECT 'Inserting Data into : silver_erp_loc_a101 ' ;
	set srt_time = sysdate();
	INSERT INTO silver_erp_loc_a101 (
	cid ,
	cntry 
	) 
	SELECT 
	REPLACE(cid,'-','') AS cid,
	CASE WHEN TRIM(cntry) IN ('US','USA') THEN  'United States'
		WHEN  TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) = ''  OR cntry IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS cntry              -- Normailize and Handling missing or Blank Country Codes
	FROM 
	bronze_erp_loc_a101 ;
    set end_time = sysdate();
	SELECT COUNT(*) FROM silver_erp_loc_a101;
    SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');


	SELECT '>>Truncating Table: silver_erp_px_cat_g1v2 ' ;
	TRUNCATE TABLE silver_erp_px_cat_g1v2 ;
	SELECT 'Inserting Data into : silver_erp_px_cat_g1v2 ' ;
    SET srt_time = sysdate();
	INSERT INTO silver_erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	SELECT 
	id , 
	cat ,
	subcat ,
	maintenance 
	FROM 
	bronze_erp_px_cat_g1v2 ;
    set end_time = sysdate();
	SELECT COUNT(*) FROM  silver_erp_px_cat_g1v2;
	SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
    set batch_end_time = sysdate();
	SELECT '==================================================================================================================================' ;
	SELECT '                                      LOADED SILVER LAYER SUCCESSFULLY                                                            ';
    SELECT CONCAT('>> TOTAL TIME TAKEN BY WHOLE BATCH : ' ,timestampdiff(second,batch_srt_time,batch_end_time) ,'second');
	SELECT '==================================================================================================================================' ;
    
END //
DELIMITER ;


