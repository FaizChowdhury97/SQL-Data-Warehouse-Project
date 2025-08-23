
/*
===============================================================
Quality Checks
===============================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================
*/


-- ==========================================    
-- Checking 'silver.crm_cust_info'
-- ==========================================

-- Checks for Null and Duplicate in Primary Key
-- Expectations: No Result

SELECT cst_id,
       COUNT(*)
FROM  silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Checks for the extra spaces
-- Expectation: No Result

SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)


-- Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-- Checking the table
SELECT * FROM silver.crm_cust_info


-- ==========================================    
-- Checking 'silver.crm_prd_info'
-- ========================================== 

-- Checks for Null and Duplicate in Primary Key
-- Expectations : No Result


SELECT prd_id,
       COUNT(*)
FROM  silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
-- Expectations : No Result

SELECT prd_nm
FROM  silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Checks for NULL or Negative Number
-- Expectations : No Result

SELECT prd_cost
FROM  silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for invalid Date Order

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt 

-- ==========================================    
-- Checking 'silver.crm_sales_details'
-- ========================================== 

-- Check the Order Number

SELECT sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Sales_cust_id_check  

SELECT *
FROM silver.crm_sales_details sd
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.crm_cust_info pi
    WHERE pi.cst_id = sd.sls_cust_id
)

-- Check the sls_prd_key

SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
	sls_price
FROM silver.crm_sales_details sd
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.crm_prd_info pi
    WHERE pi.prd_key = sd.sls_prd_key
)

-- Check the invalid date

SELECT 
NULLIF(sls_order_dt::TEXT,'0') AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
  OR LENGTH(sls_order_dt::TEXT) != 8
  OR sls_order_dt > 20500101
  OR sls_order_dt < 19000101

SELECT 
NULLIF(sls_ship_dt::TEXT,'0') AS sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0 
  OR LENGTH(sls_ship_dt::TEXT) != 8
  OR sls_ship_dt > 20500101
  OR sls_ship_dt < 19000101
  
  
SELECT 
NULLIF(sls_due_dt::TEXT,'0') AS sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
  OR LENGTH(sls_due_dt::TEXT) != 8
  OR sls_due_dt > 20500101
  OR sls_due_dt < 19000101  
  
-- Checking Invalid Date Order

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
  OR  sls_order_dt > sls_due_dt
  
-- Check Data Consistency: Between Sales, Quantity, Price
--> Sales = Quantity * Price
--> Values must not be Null, Zero or Negative

SELECT DISTINCT
      sls_sales AS sls_sales_old,
	  sls_quantity,
	  sls_price AS sls_price_old,
	  CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	       THEN sls_quantity * ABS(sls_price)
		   ELSE sls_quantity
	  END AS  sls_sales,
	  CASE WHEN sls_price IS NULL OR sls_price <=0
	       THEN sls_sales/NULLIF(sls_quantity,0)
		   ELSE sls_price
	  END AS sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0


-- ==========================================    
-- Checking 'silver.erp_cust_az12'
-- ========================================== 

-- Identify Out of Range date

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > CURRENT_DATE

-- Data Standardization & Consistance

SELECT DISTINCT
  gen AS gen_old,
  CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'FEMALE'
       WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'MALE'
	   ELSE 'N/A'
  END gen
FROM silver.erp_cust_az12

-- ==========================================    
-- Checking 'silver.erp_loc_a101'
-- ========================================== 

-- Removing Invalid Values FROM cid
SELECT
    REPLACE(l.cid, '-', '') AS cid,
FROM silver.erp_loc_a101 l
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.crm_cust_info c
    WHERE c.cst_key =  REPLACE(l.cid, '-', '') 
);

-- Stadardization & Consistancy
SELECT 
    DISTINCT cntry
FROM silver.erp_loc_a101 
GROUP BY cntry;

-- ==========================================    
-- Checking 'silver.erp_px_cat_g1v2'
-- ========================================== 

-- Check for Unwanted Spaces
SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
OR subcat != TRIM(subcat) 
OR maintenance != TRIM(maintenance)

-- Data Standardaization & Consistency
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;
