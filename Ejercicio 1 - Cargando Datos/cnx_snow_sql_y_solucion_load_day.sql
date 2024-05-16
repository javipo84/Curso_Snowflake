-- cnx snowsql DESDE CLI (NO desde la interfaz de Snowflake)
snowsql -a civicapartner.west-europe.azure -u JAVIER.FERNANDEZ@CIVICA-SOFT.COM --authenticator externalbrowser

-- Ejemplo PUT DESDE CLI (NO desde la interfaz de Snowflake)
PUT 'file://C:/Users/javier.fernandez/Desktop/DESARROLLOS/CURSO_DATA_ENGINEERING/2024/Ficheros definitivos/*' @bronze_stage;

-- Ejemplo GET DESDE CLI (NO desde la interfaz de Snowflake)
GET @bronze_stage/orders.csv.gz_0_0_0.csv.gz file://C:\\Users\\javier.fernandez\\Desktop\\;

---------------------------------
---------------------------------
---------------------------------

use role curso_data_engineering;
use warehouse wh_curso_data_engineering;

use database your_database;
use schema your_schema;

COPY INTO products from @bronze_stage/products.csv.gz
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
select * from products;

COPY INTO promos from @bronze_stage/promos.csv.gz
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);
select * from promos;

---------------------------------

use database CURSO_DATA_ENGINEERING_2024;
use schema bronze;

list @bronze_stage;

COPY INTO your_database.your_schema.addresses from @bronze_stage/addresses.csv.gz
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ';' SKIP_HEADER = 1);
select * from database.schema.addresses;

COPY INTO your_database.your_schema.events from @bronze_stage/events.csv.gz
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ';' SKIP_HEADER = 1);
select * from database.schema.events;

COPY INTO your_database.your_schema.order_items from @bronze_stage/order_items.csv.gz
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ';' SKIP_HEADER = 1);
select * from database.schema.order_items;

COPY INTO your_database.your_schema.orders from @bronze_stage/orders.csv.gz
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ';' SKIP_HEADER = 1);
select * from database.schema.orders;

COPY INTO your_database.your_schema.users from @bronze_stage/users.csv.gz
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ';' SKIP_HEADER = 1);
select * from database.schema.users;

---------------------------------
-- Ejemplo file format y query a stage

CREATE OR REPLACE FILE FORMAT my_csv
  TYPE = CSV
  FIELD_DELIMITER = ';'
  SKIP_HEADER = 1;

SELECT $1, $2, $3, $4, $5  FROM @bronze_stage/addresses.csv.gz (file_format => 'my_csv');