/*
====================================================================================
DDL SCRIPT: CREATE SILVER TABLES
====================================================================================
Purpose: Create tables in the 'bronze' schema, use check if table exist for each table

*/

--CHECK IF TABLE EXIST
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

--CREATE TABLE
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(25),
	cst_firstname NVARCHAR(25),
	cst_lastname NVARCHAR(25),
	cst_marital_status NVARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost DECIMAL(10,2),
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    [sls_ord_num] VARCHAR(50),
    [sls_prd_key] VARCHAR(50),
    [sls_cust_id] INT,
    [sls_order_dt] DATE,
    [sls_ship_dt] DATE,
    [sls_due_dt] DATE,
    [sls_sales] DECIMAL(30,2),
    [sls_quantity] INT,
    [sls_price] DECIMAL(30,2) ,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_CUST_AZ12','U') IS NOT NULL
    DROP TABLE silver.erp_CUST_AZ12;

CREATE TABLE silver.erp_CUST_AZ12(
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(20),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_LOC_A101','U') IS NOT NULL
    DROP TABLE silver.erp_LOC_A101;

CREATE TABLE silver.erp_LOC_A101(
    CID VARCHAR(50),
    CNTRY VARCHAR(20),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
    DROP TABLE silver.erp_PX_CAT_G1V2;

CREATE TABLE silver.erp_PX_CAT_G1V2(
    ID VARCHAR(20),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(20),
    MAINTENANCE VARCHAR(20),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);