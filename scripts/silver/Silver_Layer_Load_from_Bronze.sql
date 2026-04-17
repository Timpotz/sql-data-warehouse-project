--STORED PROCEDURE FOR LOADING/BUILDING THE SILVER LAYER

/*
====================================================================================
Stored Procedure: Load Cleaned Data from bronze into Silver Layerr (Source-> Bronze)
====================================================================================
Purpose: This stored procedure loads data into the silver schema from bronze layer.
	It performs the following actions:
	- Truncate(empties) the silver table before loading data.
	- Uses INSERT INTO SELECT command to load data from bronze tables to silver tables

	Parameters:
	None.
	This stored procedure does not accept any parameters or return any values

	Usage Example:
	EXEC silver.load_silver

*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        SET @batch_start_time=GETDATE();
		PRINT '========================';
		PRINT 'Loading Silver Layer';
		PRINT '========================';

		PRINT '------------------------';
		PRINT'Loading CRM Tables';
		PRINT '------------------------';

		SET @start_time= GETDATE();
		PRINT'>> Truncating Table: silver.crm_cust_info';
        -- [silver].[crm_cust_info]
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT'>> Inserting Data into Table: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info
        (	cst_id,
	        cst_key,
	        cst_firstname,
	        cst_lastname,
	        cst_marital_status,
	        cst_gndr,
	        cst_create_date
        )
        SELECT
	        cst_id,

	        cst_key,
	        TRIM(cst_firstname) cst_firstname,
	        TRIM(cst_lastname) cst_lastname,
	        CASE 
		        WHEN UPPER(cst_marital_status) ='S' THEN 'Single'
		        WHEN UPPER(cst_marital_status)= 'M' THEN 'Married'
		        ELSE 'Unknown'
	        END cst_marital_status,
	        CASE 
		        WHEN UPPER(cst_gndr) ='M' THEN 'Male'
		        WHEN UPPER(cst_gndr)= 'F' THEN 'Female'
		        ELSE 'Other'
	        END cst_gndr,
	        cst_create_date
        FROM(
	        SELECT
	        *,
	        -- USING WINDOW FUNCTION TO RANK THE PRIMARY KEYS AND ORDERED BY THE DATE OF CREATE DATE FOR MOST RECENT RECORD
	        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag
	        FROM bronze.crm_cust_info
        ) t 
        WHERE flag = 1 AND cst_id IS NOT NULL --SELECT THE MOST RECENT RECORD
        SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))
        
        -- [silver].[crm_prd_info]
        SET @start_time= GETDATE();
        PRINT'>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt

        )
        SELECT 
            [prd_id],
            REPLACE(SUBSTRING([prd_key],1,5),'-','_') AS cat_id,
            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
            [prd_nm],
            ISNULL([prd_cost],0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Sportswear'
                WHEN 'M' THEN 'Mountain'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            [prd_start_dt],
            DATEADD(DAY,-1,LEAD(prd_start_dt,1)OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt
        FROM [DataWareHouse].[bronze].[crm_prd_info]
        SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))


        -- [silver].[crm_sales_details]
        SET @start_time= GETDATE();
        PRINT'>> Truncating Table: [silver].[crm_sales_details]';
        TRUNCATE TABLE [silver].[crm_sales_details];
        INSERT INTO silver.crm_sales_details([sls_ord_num],[sls_prd_key],[sls_cust_id],sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,[sls_quantity],sls_price)
        SELECT 
            [sls_ord_num]
            ,[sls_prd_key]
            ,[sls_cust_id]
            ,STUFF(STUFF(sls_order_dt,5,0,'-'),8,0,'-') AS sls_order_dt
            ,STUFF(STUFF(sls_ship_dt,5,0,'-'),8,0,'-') AS sls_ship_dt
            ,STUFF(STUFF(sls_due_dt,5,0,'-'),8,0,'-') AS sls_due_dt
            ,CASE
            WHEN sls_sales <=0  OR sls_sales IS NULL or sls_sales!=sls_quantity* ABS(sls_price) THEN sls_quantity*ABS(sls_price)
            ELSE sls_sales END sls_sales
            ,[sls_quantity]
            ,CASE 
            WHEN sls_price < 0 THEN CAST(ABS(sls_price)AS DECIMAL (20,2))
            WHEN sls_price IS NULL OR sls_sales = 0 THEN ABS(CAST(sls_sales/sls_quantity AS DECIMAL(20,2)))
            ELSE sls_price
            END AS sls_price
        FROM [bronze].[crm_sales_details]
        SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))

        -- [silver].[erp_CUST_AZ12]
        SET @start_time= GETDATE();
        PRINT'>> Truncating Table: [silver].[erp_CUST_AZ12]';
        TRUNCATE TABLE [silver].[erp_CUST_AZ12];
        INSERT INTO silver.erp_CUST_AZ12(CID,BDATE,GEN)
        SELECT 
	        SUBSTRING(CID,4,10) AS CID
	        --,LEN(SUBSTRING(CID,4,10))  AS CID_len
	        ,CASE
		        WHEN BDATE> GETDATE() THEN NULL
		        ELSE BDATE
	        END BDATE
	        ,CASE 
		        WHEN UPPER(TRIM(GEN)) IS NULL OR GEN = ' ' THEN 'n/a'
		        WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
		        WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
		        ELSE GEN
	        END GEN
        FROM [DataWareHouse].[bronze].[erp_CUST_AZ12]
        SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))

        -- [silver].[erp_LOC_A101]
        SET @start_time= GETDATE();
        PRINT'>> Truncating Table: silver.erp_LOC_A101';
        TRUNCATE TABLE silver.erp_LOC_A101;
        INSERT INTO silver.erp_LOC_A101(CID,CNTRY)
        SELECT  
            REPLACE(CID,'-','') AS CID
           ,CASE 
                WHEN CNTRY IS NULL OR CNTRY=' ' THEN 'n/a'
                WHEN UPPER(TRIM(CNTRY)) IN ('USA','US') THEN 'United States'
                WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'
                ELSE TRIM(CNTRY)
            END CNTRY
        FROM [DataWareHouse].[bronze].[erp_LOC_A101]
        SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))


        -- [silver].[erp_PX_CAT_G1V2]
        SET @start_time= GETDATE();
        PRINT'>> Truncating Table: [silver].[erp_PX_CAT_G1V2]';
        TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
        INSERT INTO silver.erp_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
        SELECT 
               [ID]
              ,[CAT]
              ,[SUBCAT]
              ,[MAINTENANCE]
          FROM [DataWareHouse].[bronze].[erp_PX_CAT_G1V2]
          SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))

        --Time for loading the whole batch
		SET @batch_end_time=GETDATE();
		PRINT CONCAT('>> Batch Load Duration: ', DATEDIFF(SECOND, @batch_start_time, @batch_end_time))

    END TRY

    BEGIN CATCH
    PRINT '===================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAIR';
	PRINT CONCAT('Error Message: ', ERROR_MESSAGE());
	PRINT CONCAT('Error Number: ', ERROR_NUMBER());
	PRINT '===================';
    END CATCH
END