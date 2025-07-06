CREATE INDEX IF NOT EXISTS idx_orders_user
    ON orders(user_id);

CREATE INDEX IF NOT EXISTS idx_items_order
    ON order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_items_product
    ON order_items(product_id);