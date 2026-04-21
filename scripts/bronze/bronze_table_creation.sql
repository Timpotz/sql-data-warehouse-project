
/*
====================================================================================
DDL SCRIPT: CREATE BRONZE TABLES
====================================================================================
Purpose: Create tables in the 'bronze' schema, use check if table exist for each table
*/

--CHECK IF TABLE EXIST
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

--CREATE TABLE
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(25),
	cst_firstname NVARCHAR(25),
	cst_lastname NVARCHAR(25),
	cst_marital_status NVARCHAR(2),
	cst_gndr VARCHAR(2),
	cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost DECIMAL(10,2),
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    [sls_ord_num] VARCHAR(50),
    [sls_prd_key] VARCHAR(50),
    [sls_cust_id] INT,
    [sls_order_dt] INT,
    [sls_ship_dt] INT,
    [sls_due_dt] INT,
    [sls_sales] DECIMAL(30,2),
    [sls_quantity] INT,
    [sls_price] DECIMAL(30,2) 
);

IF OBJECT_ID('bronze.erp_CUST_AZ12','U') IS NOT NULL
    DROP TABLE bronze.erp_CUST_AZ12;

CREATE TABLE bronze.erp_CUST_AZ12(
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(20)
);

IF OBJECT_ID('bronze.erp_LOC_A101','U') IS NOT NULL
    DROP TABLE bronze.erp_LOC_A101;

CREATE TABLE bronze.erp_LOC_A101(
    CID VARCHAR(50),
    CNTRY VARCHAR(20)
);

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
    DROP TABLE bronze.erp_PX_CAT_G1V2;

CREATE TABLE bronze.erp_PX_CAT_G1V2(
    ID VARCHAR(20),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(20),
    MAINTENANCE VARCHAR(20)
);
