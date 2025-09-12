-- Explore all objects in the DataBase 
SELECT * FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'datawarehouse';

-- Explore all Column in the DataBase 
SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'silver_crm_cust_info';