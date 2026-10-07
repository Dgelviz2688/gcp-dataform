CREATE SCHEMA IF NOT EXISTS raw_data;

-- 1. Tabla de USUARIOS (Dimensión) - 50,000 usuarios
CREATE OR REPLACE TABLE raw_data.users AS
SELECT
  id AS user_id,
  CONCAT('User_', id) AS name,
  CASE WHEN RAND() > 0.9 THEN 'VIP' ELSE 'Standard' END AS membership_tier, -- Para usar en Lab 5
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND() * 365*3) AS INT64) DAY) AS signup_date
FROM UNNEST(GENERATE_ARRAY(1, 50000)) AS id;

-- 2. Tabla de PRODUCTOS (Dimensión) - 500 productos
CREATE OR REPLACE TABLE raw_data.products AS
SELECT
  id AS product_id,
  CONCAT('Product_', id) AS product_name,
  CASE CAST(FLOOR(RAND() * 5) AS INT64)
    WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing' WHEN 2 THEN 'Home' WHEN 3 THEN 'Toys' ELSE 'Sports'
  END AS category,
  ROUND(10 + RAND() * 200, 2) AS cost
FROM UNNEST(GENERATE_ARRAY(1, 500)) AS id;

-- 3. Tabla de ORDENES (Hechos) - 1 Millón de filas (~100MB+)
CREATE OR REPLACE TABLE raw_data.orders AS
WITH generator AS (
  SELECT
    id AS order_id,
    CAST(FLOOR(1 + RAND() * 50000) AS INT64) AS user_id,
    CAST(FLOOR(1 + RAND() * 500) AS INT64) AS product_id,
    ROUND(20 + RAND() * 500, 2) AS amount, -- Precio de venta
    CASE CAST(FLOOR(RAND() * 5) AS INT64)
      WHEN 0 THEN 'PENDING' WHEN 1 THEN 'SHIPPED' WHEN 2 THEN 'DELIVERED' WHEN 3 THEN 'RETURNED' ELSE 'CANCELLED'
    END AS status,
    CASE CAST(FLOOR(RAND() * 5) AS INT64)
      WHEN 0 THEN 'US' WHEN 1 THEN 'ES' WHEN 2 THEN 'MX' WHEN 3 THEN 'FR' ELSE 'UK'
    END AS country, -- Para el Lab 4 de Regiones
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND() * 365*2) AS INT64) HOUR) AS created_at
  FROM UNNEST(GENERATE_ARRAY(1, 1000000)) AS id
)
SELECT * FROM generator;