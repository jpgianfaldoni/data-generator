CREATE TABLE banana (
  product_id STRING NOT NULL COMMENT 'Unique product identifier',
  product_name STRING NOT NULL COMMENT 'Name of the product',
  price DECIMAL(10,2) NOT NULL COMMENT 'Product price in USD',
  category STRING COMMENT 'Product category',
  in_stock BOOLEAN NOT NULL COMMENT 'Whether product is currently in stock',
  last_updated TIMESTAMP NOT NULL COMMENT 'When the product information was last updated')
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.changeDataFeed' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')