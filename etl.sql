-- CREATE EXTERNAL TABLE IF NOT EXISTS washing_machine_csv (
--     product_name STRING,
--     brand_name_extracted STRING,
--     price_rs DOUBLE,
--     washing_capacity_kg DOUBLE,
--     max_spin_rpm DOUBLE,
--     color STRING,
--     function_type STRING,
--     inbuilt_heater STRING,
--     washing_method STRING,
--     product_url STRING
-- )
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
-- WITH SERDEPROPERTIES (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""
-- )
-- LOCATION 's3://washing-machine-raw-data/data/'
-- TBLPROPERTIES ('skip.header.line.count'='1');
 
SELECT *
FROM washing_machine_csv
LIMIT 10;

