/*
=========================================================================================================================================================
DDL Script : Create Bronze Tables
=========================================================================================================================================================
Script Purpose :
  This script creates tables in the 'bronze' schema , dropping existing tables
  if they already exist.
  Run this script to re-define the DDL structure of 'bronze Tables
=========================================================================================================================================================
*/
USE datawarehouse ;

DROP TABLE IF EXISTS bronze_crm_cust_info ;
CREATE TABLE bronze_crm_cust_info (
	cst_id INT ,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50) ,
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE 
);

DROP TABLE IF EXISTS bronze_crm_prod_info ;
CREATE TABLE bronze_crm_prod_info (
	prd_id INT ,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT ,
    prd_line VARCHAR(50),
    prd_start_date DATETIME ,
    prd_end_date DATETIME
);

DROP TABLE IF EXISTS bronze_crm_sales_detail ;
CREATE TABLE bronze_crm_sales_detail (
	sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50) , 
    sls_cust_id INT ,
    sls_order_dt INT ,
    sls_ship_dt INT ,
    sls_due_dt INT , 
    sls_sales INT ,
    sls_quantity INT ,
    sls_price INT    
);

DROP TABLE IF EXISTS bronze_erp_loc_a101 ;
CREATE TABLE bronze_erp_loc_a101(
	cid VARCHAR(50) ,
    cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze_erp_cust_az12 ;
CREATE TABLE bronze_erp_cust_az12(
	cid VARCHAR(50) ,
    bdate DATE ,
    gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze_erp_px_cat_g1v2 ;
CREATE TABLE bronze_erp_px_cat_g1v2 (
	id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);

DELIMITER //
CREATE  PROCEDURE load_bronze_layer_data ()
BEGIN
	DECLARE srt_time DATETIME ;
    DECLARE end_time DATETIME ;
    declare batch_srt_time DATETIME ;
	SELECT '==================================================================================================================================' ;
	SELECT '                                      LOADING BRONZE LAYER                                                                        ';
	SELECT '==================================================================================================================================' ;

	SET batch_srt_time = sysdate();
	SELECT '==================================================================================================================================' ;
	SELECT '                                      LOADING CRM TABLES                                                                          ';
	SELECT '==================================================================================================================================' ;
	SELECT '>> TRUNCATING THE TABLE : bronze_crm_cust_info ' ;
    SET srt_time = sysdate() ;
	TRUNCATE TABLE bronze_crm_cust_info;
	SELECT '>> INSERTING THE DATA INTO TABLE : bronze_crm_cust_info ' ;

	LOAD DATA INFILE 'D:/Data WearHouse Project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	INTO TABLE bronze_crm_cust_info
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS
	(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date );
    SET end_time = sysdate();
    SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
	SELECT count(*) FROM bronze_crm_cust_info;

	SELECT '>> TRUNCATING THE TABLE : bronze_crm_prod_info ' ;
	TRUNCATE TABLE bronze_crm_prod_info  ;
	SELECT '>> INSERTING THE DATA INTO TABLE : bronze_crm_prod_info ' ;
    SET srt_time = sysdate();
	LOAD DATA INFILE 'D:/Data WearHouse Project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	INTO TABLE bronze_crm_prod_info 
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS 
	(prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt);
    SET end_time = sysdate();
	SELECT COUNT(*) FROM bronze_crm_prod_info ;
	SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
    
	SELECT '>> TRUNCATING THE TABLE : bronze_crm_sales_detail ' ;
	TRUNCATE TABLE bronze_crm_sales_detail ;
	SELECT '>> INSERTING THE DATA INTO TABLE : bronze_crm_sales_detail ' ;
    set srt_time = sysdate();
	LOAD DATA INFILE 'D:/Data WearHouse Project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	INTO TABLE bronze_crm_sales_detail
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS 
	(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price);
    set end_time = sysdate();
	SELECT COUNT(*) FROM bronze_crm_sales_detail;
    SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');

	SELECT '==================================================================================================================================' ;
	SELECT '                                      LOADING ERP TABLES                                                                          ';
	SELECT '==================================================================================================================================' ;

	SELECT '>> TRUNCATING THE TABLE : bronze_erp_loc_a101  ' ;
	TRUNCATE TABLE bronze_erp_loc_a101 ;
	SELECT '>> INSERTING THE DATA INTO TABLE : bronze_erp_loc_a101  ' ;
    set srt_time = sysdate();
	LOAD DATA INFILE 'D:/Data WearHouse Project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	INTO TABLE bronze_erp_loc_a101
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS 
	(CID,CNTRY);
    set end_time = sysdate();
	SELECT COUNT(*) FROM bronze_erp_loc_a101;
    SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
    

	SELECT '>> TRUNCATING THE TABLE : bronze_erp_cust_az12  ' ;
	TRUNCATE TABLE bronze_erp_cust_az12;
	SELECT '>> INSERTING THE TABLE : bronze_erp_cust_az12  ' ;
    set srt_time = sysdate();
	LOAD DATA INFILE 'D:/Data WearHouse Project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	INTO TABLE bronze_erp_cust_az12
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS
	(CID,BDATE,GEN);
    set end_time = sysdate();
	SELECT COUNT(*) FROM bronze_erp_cust_az12; 
	SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
    
	SELECT '>> TRUNCATING THE TABLE : bronze_erp_px_cat_g1v2 ' ;
	TRUNCATE TABLE bronze_erp_px_cat_g1v2 ;
	SELECT '>> INSERTING THE DATA INTO TABLE : bronze_erp_px_cat_g1v2 ' ;
	set srt_time = sysdate();
    LOAD DATA INFILE 'D:/Data WearHouse Project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	INTO TABLE bronze_erp_px_cat_g1v2 
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS 
	(ID,CAT,SUBCAT,MAINTENANCE);
    set end_time = sysdate();
	SELECT COUNT(*) FROM  bronze_erp_px_cat_g1v2;
	SELECT CONCAT('>> TOTAL TIME TAKEN TO INSERT DATA INTO TABLE : ' ,timestampdiff(second,srt_time,end_time) ,'second');
    set batch_end_time = sysdate();
	SELECT '==================================================================================================================================' ;
	SELECT '                                      LOADED BRONZE LAYER SUCCESSFULLY                                                            ';
    SELECT CONCAT('>> TOTAL TIME TAKEN BY WHOLE BATCH : ' ,timestampdiff(second,batch_srt_time,batch_end_time) ,'second');
	SELECT '==================================================================================================================================' ;
    
END //
DELIMITER ;

CALL load_bronze_layer_data();
