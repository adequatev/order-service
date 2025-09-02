CREATE TABLE orders (
    order_uid        UUID PRIMARY KEY,
    track_number     VARCHAR(255) NOT NULL,
    entry            VARCHAR(100) NOT NULL,
    locale           VARCHAR(10),
    internal_signature VARCHAR(255),
    customer_id      VARCHAR(255),
    delivery_service VARCHAR(100),
    shardkey         SMALLINT,
    sm_id            INTEGER,
    date_created     TIMESTAMPTZ NOT NULL,
    oof_shard        SMALLINT
);

-- 1:1 rel to orders
CREATE TABLE deliveries (
    id UUID PRIMARY KEY,
    order_uid UUID NOT NULL UNIQUE,
    name      VARCHAR(255) NOT NULL,
    phone     VARCHAR(50) NOT NULL,
    zip       VARCHAR(20),
    city      VARCHAR(255),
    address   VARCHAR(255),
    region    VARCHAR(255),
    email     VARCHAR(255)
);

-- 1:1 rel to orders
CREATE TABLE payments (
    id UUID PRIMARY KEY,
    order_uid    UUID NOT NULL UNIQUE,
    transaction  UUID NOT NULL,
    request_id   UUID,
    currency     CHAR(3) NOT NULL,
    provider     VARCHAR(100),
    amount       INTEGER NOT NULL,
    payment_dt   BIGINT NOT NULL,
    bank         VARCHAR(100),
    delivery_cost INTEGER,
    goods_total   INTEGER,
    custom_fee    INTEGER
);

-- 1:N rel to orders
CREATE TABLE items (
    id          UUID PRIMARY KEY,
    order_uid   UUID NOT NULL,
    chrt_id     BIGINT NOT NULL,
    track_number VARCHAR(255) NOT NULL,
    price       INTEGER NOT NULL,
    rid         UUID NOT NULL,
    name        VARCHAR(255) NOT NULL,
    sale        INTEGER,
    size        VARCHAR(50),
    total_price INTEGER,
    nm_id       BIGINT,
    brand       VARCHAR(255),
    status      INTEGER
);

ALTER TABLE deliveries
    ADD CONSTRAINT fk_deliveries_orders
    FOREIGN KEY (order_uid) REFERENCES orders(order_uid) ON DELETE RESTRICT;

ALTER TABLE payments
    ADD CONSTRAINT fk_payments_orders
    FOREIGN KEY (order_uid) REFERENCES orders(order_uid) ON DELETE RESTRICT;

ALTER TABLE items
    ADD CONSTRAINT fk_items_orders
    FOREIGN KEY (order_uid) REFERENCES orders(order_uid) ON DELETE RESTRICT;
