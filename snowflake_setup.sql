
CREATE DATABASE IF NOT EXISTS RETAIL_ANALYTICS;
USE DATABASE RETAIL_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS ICEBERG_SCHEMA;
USE SCHEMA ICEBERG_SCHEMA;


CREATE OR REPLACE EXTERNAL VOLUME iceberg_ext_vol
    STORAGE_LOCATIONS = (
        (
            NAME             = 'us-east-1-iceberg'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://rev1-249954438267-us-east-1-an/iceberg/'
            STORAGE_AWS_ROLE_ARN = '<YOUR_IAM_ROLE_ARN>'
        )
    );

DESC EXTERNAL VOLUME iceberg_ext_vol;



CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_integration
    CATALOG_SOURCE    = GLUE
    CATALOG_NAMESPACE = 'iceberg_catalog_db'
    TABLE_FORMAT      = ICEBERG
    GLUE_AWS_ROLE_ARN = '<YOUR_IAM_ROLE_ARN>'
    GLUE_CATALOG_ID   = '<YOUR_AWS_ACCOUNT_ID>'
    GLUE_REGION       = 'us-east-1'
    ENABLED           = TRUE;

DESC INTEGRATION glue_catalog_integration;



CREATE OR REPLACE ICEBERG TABLE customers
    CATALOG            = 'glue_catalog_integration'
    EXTERNAL_VOLUME    = 'iceberg_ext_vol'
    CATALOG_NAMESPACE  = 'iceberg_catalog_db'
    CATALOG_TABLE_NAME = 'customers';

CREATE OR REPLACE ICEBERG TABLE products
    CATALOG            = 'glue_catalog_integration'
    EXTERNAL_VOLUME    = 'iceberg_ext_vol'
    CATALOG_NAMESPACE  = 'iceberg_catalog_db'
    CATALOG_TABLE_NAME = 'products';

CREATE OR REPLACE ICEBERG TABLE orders
    CATALOG            = 'glue_catalog_integration'
    EXTERNAL_VOLUME    = 'iceberg_ext_vol'
    CATALOG_NAMESPACE  = 'iceberg_catalog_db'
    CATALOG_TABLE_NAME = 'orders';



SELECT * FROM customers LIMIT 5;
SELECT * FROM products  LIMIT 5;
SELECT * FROM orders    LIMIT 5;

-- Record count sanity check
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'products',                COUNT(*) FROM products
UNION ALL
SELECT 'orders',                  COUNT(*) FROM orders
ORDER BY table_name;
