CREATE INDEX idx_deliveries_order_uid ON deliveries USING btree(order_uid);
CREATE INDEX idx_payments_order_uid   ON payments   USING btree(order_uid);
CREATE INDEX idx_items_order_uid      ON items      USING btree(order_uid);