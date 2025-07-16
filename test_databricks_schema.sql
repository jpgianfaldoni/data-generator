CREATE TABLE main.bronze.sales_data (
    transaction_id BIGINT NOT NULL COMMENT 'Unique transaction identifier',
    customer_id BIGINT NOT NULL COMMENT 'Customer reference',
    product_id STRING NOT NULL COMMENT 'Product SKU',
    quantity INT NOT NULL COMMENT 'Number of items purchased',
    unit_price DECIMAL(10,2) NOT NULL COMMENT 'Price per unit in USD',
    total_amount DECIMAL(12,2) NOT NULL COMMENT 'Total transaction amount',
    transaction_date TIMESTAMP NOT NULL COMMENT 'When the transaction occurred',
    payment_method STRING COMMENT 'Payment method used',
    discount_applied BOOLEAN NOT NULL COMMENT 'Whether discount was applied',
    sales_rep_id STRING COMMENT 'Sales representative identifier'
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
)
PARTITIONED BY (transaction_date)
LOCATION '/mnt/datalake/bronze/sales_data'; 