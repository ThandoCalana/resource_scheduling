USE GIT_HUB_DB;

CREATE OR REPLACE TABLE orders (
    order_id VARCHAR(10),
    product_name VARCHAR(40),
    order_value INTEGER,
    product_price INTEGER,
    order_date TIMESTAMP_NTZ
);