-- Удаляем внешние ключи
ALTER TABLE items DROP CONSTRAINT IF EXISTS fk_items_orders;
ALTER TABLE payments DROP CONSTRAINT IF EXISTS fk_payments_orders;
ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS fk_deliveries_orders;

-- Дропаем таблицы
DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS deliveries;
DROP TABLE IF EXISTS orders;
