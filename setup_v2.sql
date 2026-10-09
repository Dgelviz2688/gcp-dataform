-- Generar Dimension USERS
CREATE OR REPLACE TABLE raw_data.users AS
SELECT
  id AS user_id,
  CONCAT('User_', id) AS name,
  CASE WHEN RAND() > 0.9 THEN 'VIP' ELSE 'Standard' END AS membership_tier,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND() * 365*3) AS INT64) DAY) AS signup_date
FROM UNNEST(GENERATE_ARRAY(1, 50000)) AS id;

-- Generar Dimension PRODUCTS
CREATE OR REPLACE TABLE raw_data.products AS
SELECT
  id AS product_id,
  CONCAT('Product_', id) AS product_name,
  CASE CAST(FLOOR(RAND() * 4) AS INT64)
    WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing' WHEN 2 THEN 'Home' ELSE 'Toys'
  END AS category,
  ROUND(10 + RAND() * 100, 2) AS cost
FROM UNNEST(GENERATE_ARRAY(1, 100)) AS id;