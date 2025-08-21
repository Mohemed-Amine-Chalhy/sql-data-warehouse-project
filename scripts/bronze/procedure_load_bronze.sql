/*
-- ===================================================================================
-- Stored Procedure:Load_bronze_layer (Source -> Bronze)
-- ===================================================================================
-- Purpose: This procedure performs a full reload of the bronze layer tables.
--
-- Key Actions:
-- 1. Truncates existing data.
-- 2. Bulk loads data from CSV files.
-- 3. Handles errors and logs execution time.
-- ===================================================================================
*/


ALTER   PROCEDURE [bronze].[load_bronze] AS 

BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME;
	SET @start_time = GETDATE();
	BEGIN TRY
		PRINT '======================================';
		PRINT 'Loading Bronze Layer';
		PRINT '======================================';

		PRINT '--------------------------------------';
		PRINT 'Loading CRM TABLES';
		PRINT '--------------------------------------';
		TRUNCATE TABLE  bronze.crm_cust_info;
		BULK INSERT		bronze.crm_cust_info
		FROM 'C:\Users\CHALHY\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);


		TRUNCATE TABLE  bronze.crm_prd_info;
		BULK INSERT		bronze.crm_prd_info
		FROM 'C:\Users\CHALHY\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		TRUNCATE TABLE  bronze.crm_sales_details;
		BULK INSERT		bronze.crm_sales_details
		FROM 'C:\Users\CHALHY\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		PRINT '--------------------------------------';
		PRINT 'Loading ERP TABLES';
		PRINT '--------------------------------------';

		TRUNCATE TABLE  bronze.erp_cust_az12;
		BULK INSERT		bronze.erp_cust_az12
		FROM 'C:\Users\CHALHY\Documents\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);


		TRUNCATE TABLE  bronze.erp_loc_a101;
		BULK INSERT		bronze.erp_loc_a101
		FROM 'C:\Users\CHALHY\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		TRUNCATE TABLE  bronze.erp_px_cat_g1v2;
		BULK INSERT		bronze.erp_px_cat_g1v2
		FROM 'C:\Users\CHALHY\Documents\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
	SET @end_time = GETDATE();

	END TRY
	BEGIN CATCH
	PRINT  '======================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'ERROR MESSAGE ' + CAST(ERROR_MESSAGE() AS NVARCHAR);
	PRINT 'ERROR NUMBER ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	END CATCH

	PRINT 'TIME TO LOAD	:' + CAST(DATEDIFF(second,  @start_time,	@end_time) AS NVARCHAR);
END

