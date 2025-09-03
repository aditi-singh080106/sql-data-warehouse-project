/*
=======================================================================================
Create Database 
=======================================================================================
Script Pupose :
  This script create a new 'datawearhouse' after checking if it already exists.
  If the databasr exists it is dropped and recreated. Additionally , the script use prefix for each table
('bronze' , 'silver' , 'gold')

WARNING:
  Runnning this script will drop the entire 'datawarehous' database if it exists.
  All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backup befor running this script.
*/

DROP DATABASE IF EXISTS datawarehous;

CREATE DATABASE IF NOT  EXISTS datawarehouse ;
USE DataWarehouse ;
-- In MySQL, there is no concept of schemas inside a database (schema = database).
-- To simulate schemas within a single database, we use schema-name prefixes on tables.
-- Like 
-- bronze_table_1 
-- bronze_table_2
-- silver_table_1 
-- gold_table_1

SHOW schemas
