/*
====================================================================================================================================================================
DDL Script : Create Silver Layer Tables 
====================================================================================================================================================================
    Script PurPose : 
      This script create table inthe 'silver' layer schema , droping existing tables if they alredy exist .
      'Run' This script re-define the DDL structure od 'bronze' layer .
====================================================================================================================================================================
*/
USE datawarehouse ;
DROP TABLE IF EXISTS silver_crm_cust_info ;
CREATE TABLE silver_crm_cust_info (
	cst_id INT ,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50) ,
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE ,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver_crm_prod_info ;
CREATE TABLE silver_crm_prod_info (
	prd_id INT ,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT ,
    prd_line VARCHAR(50),
    prd_start_date DATETIME ,
    prd_end_date DATETIME,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver_crm_sales_detail ;
CREATE TABLE silver_crm_sales_detail (
	sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50) , 
    sls_cust_id INT ,
    sls_order_dt INT ,
    sls_ship_dt INT ,
    sls_due_dt INT , 
    sls_sales INT ,
    sls_quantity INT ,
    sls_price INT    ,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver_erp_loc_a101 ;
CREATE TABLE silver_erp_loc_a101(
	cid VARCHAR(50) ,
    cntry VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver_erp_cust_az12 ;
CREATE TABLE silver_erp_cust_az12(
	cid VARCHAR(50) ,
    bdate DATE ,
    gen VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver_erp_px_cat_g1v2 ;
CREATE TABLE silver_erp_px_cat_g1v2 (
	id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
