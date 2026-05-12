EXEC bronze.load_bronze;


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
		SET @batch_start_time = GETDATE();
			PRINT '----------------------------------------------';
			PRINT 'LOADING BRONZE LAYER';
			PRINT '----------------------------------------------';
			
			
			PRINT '=================================================';
			PRINT 'LOADING CRM TABLES';
			PRINT '=================================================';

			-- CRM CUST INFO
SET @start_time = GETDATE();

		PRINT '>>> TRUNCATING TABLE : crm_cust_info';
				TRUNCATE TABLE bronze.crm_cust_info;
			PRINT '>>> INSERTING DATA INTO : crm_cust_info';
					BULK INSERT bronze.crm_cust_info
					FROM 'C:\Users\Sujith kumar\OneDrive\Documents\SQL_PROJECT\source_crm\cust_info.csv'
					WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR =',',
					TABLOCK
					);
SET @end_time = GETDATE();
PRINT '>>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)as NVARCHAR) + 'seconds';


			-- CRM PRD INFO
SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING TABLE : crm_prd_info';
				TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>>> INSERTING DATA INTO : crm_prd_info';
					BULK INSERT bronze.crm_prd_info
					FROM 'C:\Users\Sujith kumar\OneDrive\Documents\SQL_PROJECT\source_crm\prd_info.csv'
					WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR =',',
					TABLOCK
					);
SET @end_time = GETDATE();
PRINT '>>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)as NVARCHAR) + 'seconds';


			-- CRM SALES DETAILS
SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING TABLE : crm_sales_details';
				TRUNCATE TABLE bronze.crm_sales_details;
					
			PRINT '>>> INSERTING DATA INTO : crm_sales_details';	
				BULK INSERT bronze.crm_sales_details
					FROM 'C:\Users\Sujith kumar\OneDrive\Documents\SQL_PROJECT\source_crm\sales_details.csv'
					WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR =',',
					TABLOCK
					);
SET @end_time = GETDATE();
PRINT '>>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)as NVARCHAR) + 'seconds';

print '===========================================================';
print 'LOADING ERP TABLES';
print '===========================================================';


			-- ERP CUST AZ12
SET @start_time = GETDATE();
PRINT '>>> TRUNCATING TABLE : erp_cust_az12';
				TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT '>>> INSERTING DATA INTO : erp_cust_az12';
					BULK INSERT bronze.erp_cust_az12
					FROM 'C:\Users\Sujith kumar\OneDrive\Documents\SQL_PROJECT\source_erp\CUST_AZ12.csv'
					WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR =',',
					TABLOCK
					);
SET @end_time = GETDATE();
PRINT '>>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)as NVARCHAR) + 'seconds';


			-- ERP LOC A101
SET @start_time = GETDATE();
PRINT '>>> TRUNCATING TABLE : erp_loc_a101';
				TRUNCATE TABLE bronze.erp_loc_a101;
					
			PRINT '>>> INSERTING DATA INTO : erp_loc_a101';
					BULK INSERT bronze.erp_loc_a101
					FROM 'C:\Users\Sujith kumar\OneDrive\Documents\SQL_PROJECT\source_erp\LOC_A101.csv'
					WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR =',',
					TABLOCK
					);
SET @end_time = GETDATE();
PRINT '>>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)as NVARCHAR) + 'seconds';



			-- ERP PX CAT G1V2
SET @start_time = GETDATE();
PRINT '>>> TRUNCATING TABLE : erp_px_cat_g1v2';
				TRUNCATE TABLE bronze.erp_px_cat_g1v2;
					
			PRINT '>>> INSERTING DATA INTO : erp_px_cat_g1v2';
					BULK INSERT bronze.erp_px_cat_g1v2
					FROM 'C:\Users\Sujith kumar\OneDrive\Documents\SQL_PROJECT\source_erp\PX_CAT_G1V2.csv'
					WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR =',',
					TABLOCK
					);
SET @end_time = GETDATE();
PRINT '>>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)as NVARCHAR) + 'seconds';




SET @batch_end_time = GETDATE();
PRINT'--------------------------------------------------';
PRINT '>>> OVERALL BRONZE LAYER LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)as NVARCHAR) + 'seconds';

	END TRY
	BEGIN CATCH
	PRINT '======================================='
		PRINT 'ERROR OCCURED DURING LOADING THE BRONZE LAYER'
		PRINT 'ERROR MSG' + ERROR_MESSAGE();
		PRINT 'ERROR MSG' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT '======================================='

	END CATCH
END