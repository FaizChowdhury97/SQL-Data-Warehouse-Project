/* 
==========================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================
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
==========================================================
*/

-- Drop old version if it exists
DROP PROCEDURE IF EXISTS bronze.load_bronze();

-- Create New Version
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$


 DECLARE
       v_start_time TIMESTAMP;
       v_end_time TIMESTAMP;
       v_duration INTERVAL;
BEGIN

	 RAISE NOTICE '=============================';
	 RAISE NOTICE 'Loading Bronze Layer';
	 RAISE NOTICE '=============================';


     RAISE NOTICE '-----------------------------';
	 RAISE NOTICE 'Loading CRM Tables';
	 RAISE NOTICE '------------------------------'; 
     
	 -- Start ETL timer
	 v_start_time := clock_timestamp();
     RAISE NOTICE '🚀 ETL started at: %', v_start_time;
	 BEGIN
	 RAISE NOTICE '>> Truncating table: bronze.crm_cust_info';
     TRUNCATE TABLE bronze.crm_cust_info;
	 
	 RAISE NOTICE '>> Inserting table: bronze.crm_cust_info';
     COPY bronze.crm_cust_info
     FROM 'H:\Projects\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
     WITH (
       FORMAT csv,
       HEADER TRUE,
       DELIMITER ','
     );
	 
	 EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.error_log (procedure_name, error_message)
        VALUES ('load_bronze_crm_cust_info', SQLERRM);
        RAISE NOTICE '❌ Error loading bronze.crm_cust_info: %', SQLERRM;
     END;
     v_end_time := clock_timestamp();
     v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time)),2);
     RAISE INFO '✅ ETL completed at: %', v_end_time;
     RAISE INFO '⏱️ Total ETL duration: %', v_duration;
	 RAISE NOTICE '=============================';

	  -- Start ETL timer
      v_start_time := clock_timestamp();
      RAISE NOTICE '🚀 ETL started at: %', v_start_time;
	  
      BEGIN
         RAISE NOTICE '>> Truncating table: bronze.crm_prd_info';
         TRUNCATE TABLE bronze.crm_prd_info;
	  
	     RAISE NOTICE '>> Inserting table: bronze.crm_prd_info';
         COPY bronze.crm_prd_info
         FROM 'H:\Projects\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
         WITH(
             FORMAT csv,
             HEADER,
             DELIMITER ','
             );
			 
         EXCEPTION WHEN OTHERS THEN
         INSERT INTO bronze.error_log (procedure_name, error_message)
         VALUES ('load_bronze_crm_prd_info', SQLERRM);
         RAISE NOTICE '❌ Error loading bronze.crm_prd_info: %', SQLERRM;
     END;
       
	 v_end_time := clock_timestamp();
     v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time)),2);
     RAISE NOTICE '✅ ETL completed at: %', v_end_time;
     RAISE NOTICE '⏱️ Total ETL duration: %', (v_duration,2);
     RAISE NOTICE '============================='; 
	  
	  -- Start ETL timer
	  v_start_time := clock_timestamp();
      RAISE NOTICE '🚀 ETL started at: %', v_start_time;
     
	 BEGIN
       RAISE NOTICE '>> Truncating table : bronze.crm_sales_details';
       TRUNCATE TABLE bronze.crm_sales_details;  

       RAISE NOTICE '>> Inserting table: bronze.crm_sales_details';
       COPY bronze.crm_sales_details
       FROM 'H:\Projects\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
       WITH(
            FORMAT csv,
            HEADER,
            DELIMITER ','
         );
		 
		EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.error_log (procedure_name, error_message)
        VALUES ('load_bronze_crm_sales_details', SQLERRM);
        RAISE NOTICE '❌ Error loading bronze.crm_sales_details: %', SQLERRM;
     END;
	 
     v_end_time := clock_timestamp();
     v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time)),2);
     RAISE NOTICE '✅ ETL completed at: %', v_end_time;
     RAISE NOTICE '⏱️ Total ETL duration: %', v_duration;
	  
	  RAISE NOTICE '-----------------------------';
	  RAISE NOTICE 'Loading ERP Tables';
	  RAISE NOTICE '------------------------------';
      
	  -- Start ETL timer
	  v_start_time := clock_timestamp();
      RAISE NOTICE '🚀 ETL started at: %', v_start_time;
	  
	  BEGIN
	    RAISE NOTICE '>> Truncating table : bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE '>> Inserting table : bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12
        FROM 'H:\Projects\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH(
             FORMAT csv,
             HEADER,
             DELIMITER ','
        );
	    EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.error_log (procedure_name, error_message)
        VALUES ('load_bronze_erp_cust_az12', SQLERRM);
        RAISE NOTICE '❌ Error loading bronze.erp_cust_az12: %', SQLERRM;
     END;
     v_end_time := clock_timestamp();
     v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time)),2);
     RAISE NOTICE '✅ ETL completed at: %', v_end_time;
     RAISE NOTICE '⏱️ Total ETL duration: %', v_duration;
	 RAISE NOTICE '=============================';
	 
	 
      -- Start ETL timer
      v_start_time := clock_timestamp();
      RAISE NOTICE '🚀 ETL started at: %', v_start_time;

      BEGIN
        RAISE NOTICE '>> Truncating table : bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
	  
        RAISE NOTICE '>> Inserting table: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101
        FROM 'H:\Projects\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH(
             FORMAT csv,
             HEADER,
             DELIMITER ','
        );
		
		EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.error_log (procedure_name, error_message)
        VALUES ('load_bronze_erp_loc_a101', SQLERRM);
        RAISE NOTICE '❌ Error loading bronze.erp_loc_a101: %', SQLERRM;
     END;
	 
	 v_end_time := clock_timestamp();
     v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time)),2);
     RAISE NOTICE '✅ ETL completed at: %', v_end_time;
     RAISE NOTICE '⏱️ Total ETL duration: %', v_duration;
	 RAISE NOTICE '=============================';
	 
	 
	  -- Start ETL timer
      v_start_time := clock_timestamp();
      RAISE NOTICE '🚀 ETL started at: %', v_start_time;
      
	  BEGIN
        RAISE NOTICE '>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	  
	    RAISE NOTICE '>> Inserting table: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2
        FROM 'H:\Projects\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH(
             FORMAT csv,
             HEADER,
             DELIMITER ','
        );
       
	   EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.error_log (procedure_name, error_message)
        VALUES ('load_bronze_erp_px_cat_g1v2', SQLERRM);
        RAISE NOTICE '❌ Error inserting into bronze.crm_cust_info: %', SQLERRM;
     END;
	 
     v_end_time := clock_timestamp();
     v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time)),2);
     RAISE NOTICE '✅ ETL completed at: %', v_end_time;
     RAISE NOTICE '⏱️ Total ETL duration: %', v_duration;

END;
$$;

-- Call it
CALL bronze.load_bronze();
