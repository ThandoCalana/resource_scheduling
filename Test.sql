USE DATABASE GIT_HUB_DB;

CREATE OR REPLACE TABLE orders (
    order_id VARCHAR(10),
    product_name VARCHAR(20),
    product_price NUMBER(38,2),
    order_value NUMBER(38,2),
    order_date TIMESTAMP_NTZ
);