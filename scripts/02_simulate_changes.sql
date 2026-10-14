-- =============================================================================
-- 02_simulate_changes.sql
-- Simula un "nuevo día" en el sistema de origen para probar:
--   * Lab 3: la carga incremental de orders_enriched (llegan órdenes nuevas).
--   * Lab 5: el snapshot SCD Tipo 2 de usuarios (cambia el membership_tier).
-- Ejecutar DESPUÉS de un primer run completo de Dataform:
--   bash scripts/setup.sh <PROJECT_ID> --simulate
-- =============================================================================

-- 1. El 5 % de los usuarios cambia de nivel (VIP <-> Standard)
UPDATE raw_data.users
SET membership_tier = IF(membership_tier = 'VIP', 'Standard', 'VIP')
WHERE MOD(user_id, 20) = 0;

-- 2. Llegan 10.000 órdenes nuevas, posteriores al último checkpoint
INSERT INTO raw_data.orders (order_id, user_id, product_id, amount, status, country, created_at)
WITH last_order AS (
  SELECT MAX(order_id) AS max_id FROM raw_data.orders
)
SELECT
  max_id + id AS order_id,
  CAST(FLOOR(1 + RAND() * 50000) AS INT64) AS user_id,
  CAST(FLOOR(1 + RAND() * 500) AS INT64) AS product_id,
  ROUND(20 + RAND() * 500, 2) AS amount,
  'pending' AS status,
  CASE CAST(FLOOR(RAND() * 5) AS INT64)
    WHEN 0 THEN 'US' WHEN 1 THEN 'ES' WHEN 2 THEN 'MX' WHEN 3 THEN 'FR' ELSE 'UK'
  END AS country,
  CURRENT_TIMESTAMP() AS created_at
FROM last_order, UNNEST(GENERATE_ARRAY(1, 10000)) AS id;
