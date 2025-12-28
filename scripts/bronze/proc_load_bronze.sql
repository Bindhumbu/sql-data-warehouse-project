/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/



create or alter procedure bronze.load_bronze as
begin
	DECLARE @start_time Datetime ,@end_time Datetime, @batch_start_time DATETIME, @batch_end_time DATETIME;
	begin try

	SET @batch_start_time = GETDATE();

	print'================================';
	print'Loading bronze layer';
	print'================================';

	print'--------------------------------';
	print'Loading CRM tables';
	print'--------------------------------';

	set @start_time=getdate();

	print'>>> Truncating table bronze.crm_cust_info';

	truncate table bronze.crm_cust_info;

	print 'Inserting data into:bronze.crm_cust_info';
	bulk insert bronze.crm_cust_info
	from 'C:\Users\mbind\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=getdate();
	print'>> load Duration'+ cast(datediff(second ,@start_time,@end_time) as varchar)+'seconds';
	print'>>----------------';
	
	set @start_time=getdate();
	print'>>> Truncating table bronze.crm_prd_info';
	truncate table bronze.crm_prd_info;

	print 'Inserting data into:bronze.crm_prd_info';

	bulk insert bronze.crm_prd_info
	from 'C:\Users\mbind\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=getdate();
	print'>> load Duration'+ cast(datediff(second, @start_time,@end_time) as varchar)+'seconds';
	print'>>----------------';

	set @start_time=getdate();
	print'>>> Truncating table  bronze.crm_sales_details';
	truncate table bronze.crm_sales_details

	print 'Inserting data into: bronze.crm_sales_details';

	bulk insert bronze.crm_sales_details
	from 'C:\Users\mbind\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);

	set @end_time=getdate();
	print'>> load Duration'+ cast(datediff(second, @start_time,@end_time) as varchar)+'seconds';
	print'>>----------------';
	

	print'--------------------------------';
	print'Loading ERP tables';
	print'--------------------------------';

	set @start_time=getdate();
	print'>>> Truncating table bronze.erp_cust_az12';

	truncate table bronze.erp_cust_az12;

	print 'Inserting data into:bronze.erp_cust_az12';

	bulk insert bronze.erp_cust_az12
	from 'C:\Users\mbind\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=getdate();
	print'>> load Duration'+ cast(datediff(second, @start_time,@end_time) as varchar)+'seconds';
	print'>>----------------';

	set @start_time=getdate();

	print'>>> Truncating table bronze.erp_loc_a101';

	truncate table bronze.erp_loc_a101;

	print 'Inserting data into:bronze.erp_loc_a101';

	bulk insert bronze.erp_loc_a101
	from 'C:\Users\mbind\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=getdate();
	print'>> load Duration'+ cast(datediff(second, @start_time,@end_time) as varchar)+'seconds';
	print'>>----------------';

	set @start_time=getdate();

	print'>>> Truncating table bronze.erp_px_cat_g1v2';

	truncate table bronze.erp_px_cat_g1v2;

	print 'Inserting data into:bronze.erp_px_cat_g1v2';

	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\mbind\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=getdate();
	print'>> load Duration' + cast(datediff(second, @start_time,@end_time) as varchar)+'seconds';
	print'>>----------------';
	
	SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	
	end try
	begin catch
		print'======================================';
		print'Error occured during loading bronze layer'
		print'Error Message'+ Error_message();
		print'Error Message'+ cast (Error_number() as nvarchar);
		print'Error Message'+ cast(error_state() as nvarchar);
		print'======================================';
	end catch
end 

go

exec bronze.load_bronze;
