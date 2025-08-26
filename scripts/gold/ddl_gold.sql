/*
====================================================================================
DDL Script: Create Gold Views
====================================================================================

Script Purpose:
     - This script creates views for the Gold layer in the data warehouse.
     - The Gold layer represents the final dimension and fact tables (Star Schema).
     - Each view performs transformations and combines data from the Silver layer to
     - produce a clean, enriched, and business-ready dataset.

Usage:
     - These views can be queried directly for analytics and reporting.
=====================================================================================
/*

-- ====================================================================================
-- Create Dimensions: gold.dim_customers
-- ====================================================================================


-- Drop the view if it already exists
DROP VIEW IF EXISTS gold.dim_customers;

-- Create the view
CREATE VIEW gold.dim_customers AS
SELECT 
   ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
   ci.cst_id AS customer_id,
   ci.cst_key AS customer_number,
   ci.cst_firstname AS first_name,
   ci.cst_lastname AS last_name,
   la.cntry AS country,
   ci.cst_marital_status AS marital_status,
   CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr   -- CRM is the master table
	    ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
   ci.cst_create_date AS create_date,
   ca.bdate AS birth_date
   
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid


-- ====================================================================================
-- Create Dimensions: gold.dim_products
-- ====================================================================================

-- Drop the view if it already exists
DROP VIEW IF EXISTS gold.dim_products;

-- Create the view
CREATE VIEW gold.dim_products AS
SELECT
  ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,         -- Unique product identifier
    pn.prd_key AS product_number,    -- Internal product key
    pn.prd_nm AS product_name,       -- Descriptive name of the product
    pn.cat_id AS category_id,        -- Foreign key to category table
    pc.cat AS category,              -- Category name
    pc.subcat AS subcategory,        -- Subcategory name
    pc.maintenance,                  -- Maintenance info (e.g., schedule or status)
    pn.prd_cost AS cost,             -- Product cost
    pn.prd_line AS product_line,     -- Product line or grouping
    pn.prd_start_dt AS start_date    -- Launch/start date of the product
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL             -- Filter out historical products

-- ====================================================================================
-- Create Dimensions: gold.fact_sales
-- ====================================================================================

-- Drop the view if it already exists
DROP VIEW IF EXISTS gold.fact_sales;

-- Create the view
CREATE VIEW gold.fact_sales AS
SELECT 
     sd.sls_ord_num AS order_number,
	 dp.product_key,
	 dc.customer_key,
	 sd.sls_order_dt AS order_date,
	 sd.sls_ship_dt AS shipping_date,
	 sd.sls_due_dt AS due_date,
	 sd.sls_sales AS sales_amount,
	 sd.sls_quantity AS quantity,
	 sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp
ON sd.sls_prd_key = dp.product_number
