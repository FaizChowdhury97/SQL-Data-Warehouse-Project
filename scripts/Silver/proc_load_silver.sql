/*
========================================================
⚙️ Stored Procedure: Load Silver Layer (Bronze → Silver)
========================================================

Script Purpose:
     - This stored procedure performs the ETL (Extract, Transform, Load) process to populate the silver schema tables from the bronze schema.

Actions Performed:
     - Truncates Silver tables.
     - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
     - None.
     - This stored procedure does not accept any parameters or return any values.

Usage Example:
    - EXEC Silver.load_silver;
===========================================================
/*


-- 🔄 Drop the procedure if it already exists to avoid redeclaration errors
DROP PROCEDURE IF EXISTS silver.load_silver();

-- 🚀 Create the new version of the procedure
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;  -- Start time for each ETL block
    v_end_time   TIMESTAMP;  -- End time for each ETL block
    v_duration   INTERVAL;   -- Duration of each ETL block
BEGIN
    -- 🟡 Procedure Kickoff: Set the tone for structured logging
    RAISE NOTICE '=============================';
    RAISE NOTICE '🚀 Starting Silver Layer ETL';
    RAISE NOTICE '=============================';

    ------------------------------------------------------------------
    -- 🧩 Step 1: Load CRM Customer Info
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '📦 Loading CRM Customer Info at: %', v_start_time;

    BEGIN
        -- 🧹 Clear existing data to avoid duplicates
        TRUNCATE TABLE silver.crm_cust_info;

        -- 📥 Insert latest customer records, deduplicated by cst_id
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            -- 💍 Normalize marital status codes
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            -- 🚻 Normalize gender codes
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) sub
        WHERE flag_last = 1;  -- ✅ Keep only the most recent record per customer

    EXCEPTION WHEN OTHERS THEN
        -- 🛑 Log any error that occurs during this block
        INSERT INTO silver.error_log (procedure_name, error_message)
        VALUES ('load_silver_crm_cust_info', SQLERRM);
        RAISE NOTICE '❌ Error loading CRM Customer Info: %', SQLERRM;
    END;

    -- ⏱️ Log duration for CRM Customer Info load
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '✅ CRM Customer Info loaded at: %', v_end_time;
    RAISE NOTICE '⏱️ Duration: %', v_duration;

    ------------------------------------------------------------------
    -- 🧩 Step 2: Load CRM Product Info
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '📦 Loading CRM Product Info at: %', v_start_time;

    BEGIN
        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            -- 🧬 Extract and normalize category ID from product key
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            REPLACE(SUBSTRING(prd_key, 7), '-', '_'),
            prd_nm,
            COALESCE(prd_cost, 0),  -- 🧮 Default missing cost to 0
            -- 🚲 Normalize product line codes
            CASE UPPER(TRIM(prd_line))
                WHEN 'R' THEN 'Road'
                WHEN 'M' THEN 'Mountain'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END,
            CAST(prd_start_dt AS DATE),
            -- 📆 Calculate end date as one day before next start date
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 DAY' AS DATE)
        FROM bronze.crm_prd_info;

    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.error_log (procedure_name, error_message)
        VALUES ('load_silver_crm_prd_info', SQLERRM);
        RAISE NOTICE '❌ Error loading CRM Product Info: %', SQLERRM;
    END;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '✅ CRM Product Info loaded at: %', v_end_time;
    RAISE NOTICE '⏱️ Duration: %', v_duration;

    ------------------------------------------------------------------
    -- 🧩 Step 3: Load CRM Sales Details
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '📦 Loading CRM Sales Details at: %', v_start_time;

    BEGIN
        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            -- 📆 Convert valid date integers to proper DATE format
            CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
                 ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD') END,
            CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
                 ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD') END,
            CASE WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
                 ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD') END,
            -- 💰 Recalculate sales if missing or incorrect
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales END,
            sls_quantity,
            -- 🧮 Recalculate price if missing or invalid
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price END
        FROM bronze.crm_sales_details;

    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.error_log (procedure_name, error_message)
        VALUES ('load_silver_crm_sales_details', SQLERRM);
        RAISE NOTICE '❌ Error loading CRM Sales Details: %', SQLERRM;
    END;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '✅ CRM Sales Details loaded at: %', v_end_time;
    RAISE NOTICE '⏱️ Duration: %', v_duration;

    ------------------------------------------------------------------
    -- 🧩 Step 4: Load ERP Customer Info
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '📦 Loading ERP Customer Info at: %', v_start_time;

    BEGIN
        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (
            cid, bdate, gen
        )
        SELECT
            -- 🧼 Remove 'NAS' prefix from customer ID
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
            -- 📆 Nullify future birthdates
            CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END,
            -- 🚻 Normalize gender values
            CASE UPPER(TRIM(gen))
                WHEN 'F' THEN 'FEMALE'
                WHEN 'FEMALE' THEN 'FEMALE'
                WHEN 'M' THEN 'MALE'
                WHEN 'MALE' THEN 'MALE'
                ELSE 'N/A'
            END
        FROM bronze.erp_cust_az12;

    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.error_log (procedure_name, error_message)
        VALUES ('load_silver_erp_cust_az12', SQLERRM);
        RAISE NOTICE '❌ Error loading ERP Customer Info: %', SQLERRM;
    END;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '✅ ERP Customer Info loaded at: %', v_end_time;
    RAISE NOTICE '⏱️ Duration: %', v_duration;
	
END;
$$;

-- Call it
CALL silver.load_silver();
