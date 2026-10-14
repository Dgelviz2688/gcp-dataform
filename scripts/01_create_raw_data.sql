-- =============================================================================
-- 01_create_raw_data.sql
-- Genera la capa RAW sintética del e-commerce (simula la ingesta desde el OLTP).
-- Ejecutar una sola vez, antes del primer run de Dataform:
--   bash scripts/setup.sh <PROJECT_ID>
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw_data
OPTIONS (location = "US", description = "Capa RAW: datos de origen sin transformar");

-- 1. USERS (dimensión) - 50.000 usuarios, ~10 % VIP
CREATE OR REPLACE TABLE raw_data.users AS
SELECT
  id AS user_id,
  CONCAT('User_', id) AS name,
  CASE WHEN RAND() > 0.9 THEN 'VIP' ELSE 'Standard' END AS membership_tier,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND() * 365 * 3) AS INT64) DAY) AS signup_date
FROM UNNEST(GENERATE_ARRAY(1, 50000)) AS id;

-- 2. PRODUCTS (dimensión) - 500 productos en 5 categorías
CREATE OR REPLACE TABLE raw_data.products AS
SELECT
  id AS product_id,
  CONCAT('Product_', id) AS product_name,
  CASE CAST(FLOOR(RAND() * 5) AS INT64)
    WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing' WHEN 2 THEN 'Home' WHEN 3 THEN 'Toys' ELSE 'Sports'
  END AS category,
  ROUND(10 + RAND() * 200, 2) AS cost
FROM UNNEST(GENERATE_ARRAY(1, 500)) AS id;

-- 3. ORDERS (hechos) - 1.000.000 de órdenes repartidas en los últimos 2 años
CREATE OR REPLACE TABLE raw_data.orders AS
SELECT
  id AS order_id,
  CAST(FLOOR(1 + RAND() * 50000) AS INT64) AS user_id,
  CAST(FLOOR(1 + RAND() * 500) AS INT64) AS product_id,
  ROUND(20 + RAND() * 500, 2) AS amount,
  CASE CAST(FLOOR(RAND() * 5) AS INT64)
    WHEN 0 THEN 'pending' WHEN 1 THEN 'shipped' WHEN 2 THEN 'delivered' WHEN 3 THEN 'returned' ELSE 'cancelled'
  END AS status,
  CASE CAST(FLOOR(RAND() * 5) AS INT64)
    WHEN 0 THEN 'US' WHEN 1 THEN 'ES' WHEN 2 THEN 'MX' WHEN 3 THEN 'FR' ELSE 'UK'
  END AS country,
  -- 2 años expresados en segundos para obtener ~24 cohortes mensuales
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND() * 2 * 365 * 24 * 3600) AS INT64) SECOND) AS created_at
FROM UNNEST(GENERATE_ARRAY(1, 1000000)) AS id;
