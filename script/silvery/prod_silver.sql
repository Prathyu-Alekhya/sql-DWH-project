EXEC silver.load_silver

Create or alter procedure silver.load_silver AS
Begin
	declare @start_time datetime,@end_time datetime;
	Begin try
		
		print'======================================================';
		print'Loading silver Layer';
		print'======================================================';

		print'------------------------------------------------------';
		print'Loading CRM tables';
		print'------------------------------------------------------';

		set @start_time = GETDATE();

		print'>> Truncating Table: silver.crm_cust_info';
		Truncate table silver.crm_cust_info
		print'>> Inserting Data Into: silver.crm_cust_info';
		insert into silver.crm_cust_info(cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date)

		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_material_status))='M' then 'Married'
			when upper(trim(cst_material_status))='S' then 'Single'
			else 'n/a'
			end as cst_material_status,
		case when upper(trim(cst_gndr))='F' then 'Female'
			when upper(trim(cst_gndr))='M' then 'Male'
			else 'n/a'--Handling Missing Data(Fills in the blanks by adding a default value)
			end as cst_gndr,
		cst_create_date 
		from 
		--Removing Duplicates(Ensures only one record per entity by identifying and retaining the most relevant now)
		(select 
		*, 
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_list 
		from Bronze.crm_cust_info where cst_id is not null)t where flag_list=1;

		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'; 

-----------------------------------------------------------------------------------------------

--select * from silver.crm_cust_info;

-----------------------------------------------------------------------------------------------



---Insert data with quality in silver.crm_prd_info from bronze.crm_prd_info
		set @start_time = GETDATE();
		print'>> Truncating Table: silver.crm_prd_info';
		Truncate table silver.crm_prd_info
		print'>> Inserting Data Into: silver.crm_prd_info';
		insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
		select 
		prd_id,
		REPLACE(substring(prd_key,1,5),'-','_') as cat_id,--Extract category ID(derived column= create new columns based on calculations or transformations of existing ones),
		substring(prd_key,7,len(prd_key)) as prd_key,	  --Extract product key
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
			when 'M' Then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'n/a'
		end as prd_line,--Map product line codes to descriptive values
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt --Calculate end date as one b4 the next start date
		from bronze.crm_prd_info
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'; 


		----------------------------------------------------------------------------------------------------------

		--select * from silver.crm_prd_info;

		----------------------------------------------------------------------------------------------------------

		--Creating silver.crm_sales_details table
		--Check Data Consistency: Between Sales, Quantity, and Price
		-->> Sales=Quantity*Price
		-->> Values must not be NULL, Zero, or Negative.

		--Rules
		--1) If sales is negative, zero, or null, derive it using Quantity and price.
		--2) If price is zero or null, calculate it using sales and quantity.
		--3) If price is negative, convert it to positive value

		set @start_time = GETDATE();
		print'>> Truncating Table: silver.crm_sales_details';
		Truncate table silver.crm_sales_details
		print'>> Inserting Data Into: silver.crm_sales_details';
		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key, 
			sls_cust_id, 
			sls_order_dt, 
			sls_ship_dt, 
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price 
		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt=0 or len(sls_order_dt)!=8 then NULL
			else cast(cast(sls_order_dt as nvarchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then NULL
			else cast(cast(sls_ship_dt as nvarchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt=0 or len(sls_due_dt)!=8 then NULL
			else cast(cast(sls_due_dt as nvarchar) as date)
		end as sls_due_dt,
		case 
		   when sls_sales IS NULL 
		   or sls_sales <= 0 
		   or sls_sales != sls_quantity * abs(sls_price)
		   then sls_quantity * abs(sls_price)
		   else sls_sales
		end as sls_sales,
		sls_quantity,
		case 
			when sls_price IS NULL or sls_price <= 0
			then sls_sales / nullif(sls_quantity, 0)
			else sls_price
		end as sls_price
		from Bronze.crm_sales_details
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'; 


		--------------------------------------------------------------------------------------------------------------------------

		--select * from silver.crm_sales_details

		--------------------------------------------------------------------------------------------------------------------------

		--Creating silver.erp_cust_az12
		--Inserting values in the table silver.erp_cust_az12

		set @start_time = GETDATE();
		print'>> Truncating Table: silver.erp_cust_az12';
		Truncate table silver.crm_cust_info
		print'>> Inserting Data Into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12
		(
		cid,
		bdate,
		gen
		)
		select 
		case when cid like 'NAS%' then substring(cid,4,len(cid))
			else cid
		end as cid,
		case when bdate>getdate() then NULL
			else bdate
		end as bdate,
		case 
			when upper(trim(gen)) in ('F','Female') then 'Female'
			when upper(trim(gen)) in ('M', 'Male') then 'Male'
			else 'n/a'
		end as gen
		from bronze.erp_cust_az12
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'; 


		-------------------------------------------------------------------------------------------------------------------------------------

		--select * from silver.erp_cust_az12

		--------------------------------------------------------------------------------------------------------------------------------------
		--Creating silver.erp_loc_a101

		set @start_time = GETDATE();
		print'>> Truncating Table:silver.erp_loc_a101';
		Truncate table silver.erp_loc_a101
		print'>> Inserting Data Into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101(cid,cntry)
		select 
		REPLACE(cid,'-','')cid,
		case 
			when trim(cntry)='' or cntry is null then 'n/a'
			when trim(cntry)='DE' then 'Germany'
			when trim(cntry)in('US','USA') then 'United States'
			else cntry
		end as cntry
		from Bronze.erp_loc_a101
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'; 


		---------------------------------------------------------------------------------------------------------------------------------

		--select * from silver.erp_loc_a101

		---------------------------------------------------------------------------------------------------------------------------------

		--Creating and Inserting data into silver.erp_px_cat_g1v2 from bronze.erp_px_cat_g1v2

		set @start_time = GETDATE();
		print'>> Truncating Table: silver.erp_px_cat_g1v2';
		Truncate table silver.erp_px_cat_g1v2
		print'>> Inserting Data Into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2;
		set @end_time = GETDATE();
		print'>> Load Duration:'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'; 


		---------------------------------------------------------------------------------------------------------------------------------

		--select * from silver.erp_px_cat_g1v2

		----------------------------------------------------------------------------------------------------------------------------------

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


