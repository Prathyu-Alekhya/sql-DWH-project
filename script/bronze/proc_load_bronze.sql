/*
====================================================================
Stored Procedure: Load Bronze layer (Source -> Bronze)
====================================================================
Script Purpose:
	This stored procedure loads data into the 'Bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'BULK INSERT' command to laod from csv files to bronze tables.

Parameters:
	None.
	This stored procedure doesn't accept any parameters or return any values.

Usage Example:
	EXEC Bronze.load_bronze;

========================================================================

*/

Create or alter procedure bronze.load_bronze AS
Begin
	declare @start_time datetime,@end_time datetime;
	Begin try
		print'======================================================';
		print'Loading Bronze Layer';
		print'======================================================';

		print'------------------------------------------------------';
		print'Loading CRM tables';
		print'------------------------------------------------------';

		set @start_time = GETDATE();
		print'>>Truncating Table:bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print'>>Inserting Table:bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\rahul\AppData\Local\Temp\13b72ba7-9320-4cad-a2f7-23244ee299b2_sql-data-warehouse-project.zip.9b2\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'; 
		--select * from Bronze.crm_cust_info;


		set @start_time = GETDATE();
		print'>>Truncating Table:Bronze.crm_prd_info';
		truncate table Bronze.crm_prd_info;

		print'>>Inserting Table: Bronze.crm_prd_info';
		bulk insert Bronze.crm_prd_info
		from 'C:\Users\rahul\AppData\Local\Temp\88a69283-d0a4-4829-a477-1e5203d84de7_sql-data-warehouse-project.zip.de7\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'; 

		--select * from Bronze.crm_prd_info;

		set @start_time = GETDATE();
		print'>>Truncating Table:Bronze.crm_sales_details';
		truncate table Bronze.crm_sales_details;

		
		print'>>Inserting Table: Bronze.crm_sales_details';
		bulk insert Bronze.crm_sales_details
		from 'C:\Users\rahul\AppData\Local\Temp\2497a7d3-e0dd-46a2-826a-20eb0bcd6399_sql-data-warehouse-project.zip.399\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds';

		--select * from  Bronze.crm_sales_details
		set @start_time = GETDATE();
		print'>>Truncating Table:Bronze.erp_cust_az12';
		truncate table Bronze.erp_cust_az12;

		print'>>Inserting Table: Bronze.erp_cust_az12';
		bulk insert Bronze.erp_cust_az12
		from 'C:\Users\rahul\AppData\Local\Temp\21555e48-2cf0-4cbf-9315-b311568bfaa6_sql-data-warehouse-project.zip.aa6\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds';


		--select * from  Bronze.erp_cust_az12

		set @start_time = GETDATE();
		print'>>Truncating Table:Bronze.erp_loc_a101';
		truncate table Bronze.erp_loc_a101;

		print'>>Inserting Table: Bronze.erp_loc_a101';
		bulk insert Bronze.erp_loc_a101
		from 'C:\Users\rahul\AppData\Local\Temp\fcb18b4f-fedc-4316-a78a-20abbd1673f9_sql-data-warehouse-project.zip.3f9\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds';


		--select * from  Bronze.erp_loc_a101
		set @start_time = GETDATE();
		print'>>Truncating Table:Bronze.erp_px_cat_g1v2';
		truncate table Bronze.erp_px_cat_g1v2;

		print'>>Inserting Table: Bronze.erp_px_cat_g1v2';
		bulk insert Bronze.erp_px_cat_g1v2
		from 'C:\Users\rahul\AppData\Local\Temp\637d9c1c-0200-4761-ad87-5e0f255aca49_sql-data-warehouse-project.zip.a49\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds';
		--select * from  Bronze.erp_px_cat_g1v2
	End try--SQL runs the try block, and if it fails, it runs the catch block to handle the error

	begin catch--Helps to identify bottlenecks,optimize performance, monitor trends, detect issues(Track ETL Duration)
		print'==========================================';
		print'Error Occured During Loading Bronze Layer'
		print'error message'+ error_message();
		print'error message' + cast(error_number() as nvarchar);
		print'error message' + cast(error_state() as nvarchar);
		print'==========================================';
	end catch
End

EXEC Bronze.load_bronze;
