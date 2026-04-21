/*
====================================================================================
Stored Procedure: Load Data into Bronze Lair (Source-> Bronze)
====================================================================================
Purpose: This stored procedure loads data into the bronze schema from external CSV files.
	It performs the following actions:
	- Truncate(empties) the bronze table before loading data.
	- Uses BULK INSERT command to load data from CSV files to bronze tables

	Parameters:
	None.
	This stored procedure does not accept any parameters or return any values

	Usage Example:
	EXEC bronze.load_bronze

*/

USE DataWareHouse
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT '========================';
		PRINT 'Loading Bronze Lair';
		PRINT '========================';

		PRINT '------------------------';
		PRINT'Loading CRM Tables';
		PRINT '------------------------';

		SET @start_time= GETDATE();
		PRINT'>> Truncating Table: bronze.crm_cust_info';
		--TRUNCATE / REMOVE TABLE CONTENT
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>> Inserting Data into Table: bronze.crm_cust_info';
		--BULK INSERTS INTO TABLES
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'C:\Users\asus\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))

		SET @start_time= GETDATE();
		PRINT'>> Truncating Table: [bronze].[crm_prd_info]';
		--TRUNCATE / REMOVE TABLE CONTENT
		TRUNCATE TABLE [bronze].[crm_prd_info];
		PRINT'>> Inserting Data into Table: [bronze].[crm_prd_info]';
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\Users\asus\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))

		SET @start_time= GETDATE();
		PRINT'>> Truncating Table: [bronze].[crm_sales_details]';
		--TRUNCATE / REMOVE TABLE CONTENT
		TRUNCATE TABLE [bronze].[crm_sales_details];
		PRINT'>> Inserting Data into Table: [bronze].[crm_sales_details]';
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'C:\Users\asus\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))


		PRINT '------------------------';
		PRINT'Loading ERP Tables';
		PRINT '------------------------';

		SET @start_time= GETDATE();
		PRINT'>> Truncating Table: [bronze].[erp_CUST_AZ12]';
		--TRUNCATE / REMOVE TABLE CONTENT
		TRUNCATE TABLE [bronze].[erp_CUST_AZ12];
		PRINT'>> Inserting Data into Table: [bronze].[erp_CUST_AZ12]';
		BULK INSERT [bronze].[erp_CUST_AZ12]
		FROM 'C:\Users\asus\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))

		SET @start_time= GETDATE();
		PRINT'>> Truncating Table: [bronze].[erp_LOC_A101]';
		--TRUNCATE / REMOVE TABLE CONTENT
		TRUNCATE TABLE [bronze].[erp_LOC_A101];
		PRINT'>> Inserting Data into Table: [bronze].[erp_LOC_A101]';
		BULK INSERT [bronze].[erp_LOC_A101]
		FROM 'C:\Users\asus\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT CONCAT('>> Load Duration: ', DATEDIFF(SECOND, @start_time, @end_time))

		SET @start_time= GETDATE();
		PRINT'>> Truncating Table: [bronze].[erp_PX_CAT_G1V2]';
		--TRUNCATE / REMOVE TABLE CONTENT
		TRUNCATE TABLE [bronze].[erp_PX_CAT_G1V2];
		PRINT'>> Inserting Data into Table: [bronze].[erp_PX_CAT_G1V2]';
		BULK INSERT [bronze].[erp_PX_CAT_G1V2]
		FROM 'C:\Users\asus\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
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


SELECT * FROM [bronze].[erp_PX_CAT_G1V2]

SELECT COUNT(*) FROM [bronze].[crm_sales_details]
