# Transformación (Funciones y Procedimientos)

Mientras tu ingestabas los datos de prueba para que negocio pudiera tener una primera visión rápida de los mismos, uno de tus compañeros ha realizado el desarrollo para ingestar estos datos directamente desde la aplicación web, creando finalmente un repositorio centralizado en Snowflake, en la base de datos CURSO_SNOWFLAKE_DE_2023 , esquema BRONZE tenemos ya todas las tablas cargadas.

Ahora, necesitamos trabajar y pulir el dato para sacar datos relevantes a partir de él, además intentaremos que esta transformación se lance de forma sencilla, este será tu nuevo cometido.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 1: CLONING

Para asegurarnos de que todos partimos de la misma situación vamos a clonar el esquema BRONZE de CURSO_SNOWFLAKE_DE_2023 a nuestra propia base de datos, reescribiendo nuestro esquema BRONZE para que haya menos lío. Hemos visto el cloning en teoría y la sintaxis es muy sencilla. Aqui está la documentación de crear un objeto como clon de otro, https://docs.snowflake.com/en/sql-reference/sql/create-clone. Ya sabeís que en la documentación la sintaxis viene con muchas pamplinas al final el comando que solemos lanzar es mucho más corto y sencillo. 

Pista : Siempre que tengáis un objeto ya creado y lo querais recrear sobreescribiendo el antiguo podéis usar la claúsula OR REPLACE en el comando CREATE.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 2: TIME TRAVEL WAR

En este paso praticaremos el TIME TRAVEL. Daremos más detalles del ejercicio hablando, por aquí os dejo la documentación y algunos ejemplos.
doc : https://docs.snowflake.com/en/sql-reference/constructs/at-before

ejemplo:
create or replace table PEPE CLONE PEPE AT(TIMESTAMP => '2023-10-26 10:18:07.000');

También se puede tirar la select solamente para consultar algo
SELECT * FROM PEPE AT(TIMESTAMP => '2023-10-26 10:18:07.000');

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 3: CAPA SILVER

Ahora mismo todos tenemos en nuestro BRONZE nuestras tablas con el dato RAW, osea sin tratar. El siguiente paso será pasar al schema SILVER y aprovecharemos este paso para hacer algunos cambios y añadir algún campo que enriquezca nuestros datos.

En primer lugar tenemos que crear el esquema SILVER y las tablas de SILVER, os dejo aqui los CREATES que vamos a usar.

```

-- SILVER

--  ORDERS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.ORDERS (
    ORDER_ID VARCHAR(50),
    SHIPPING_SERVICE VARCHAR(20),
    SHIPPING_COST FLOAT,
    ADDRESS_ID VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ,
    Promo_Name VARCHAR(50),
    ESTIMATED_DELIVERY_AT TIMESTAMP_NTZ,
    ORDER_COST FLOAT,
    USER_ID VARCHAR(50),
    ORDER_TOTAL FLOAT,
    DELIVERED_AT TIMESTAMP_NTZ,
    TRACKING_ID VARCHAR(50),
    STATUS VARCHAR(20),
    Delivery_time_hours INT
);

-- EVENTS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.EVENTS (
    EVENT_ID VARCHAR(50),
    PAGE_URL VARCHAR(200),
    EVENT_TYPE VARCHAR(50),
    USER_ID VARCHAR(50),
    PRODUCT_ID VARCHAR(50),
    SESSION_ID VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ,
    ORDER_ID VARCHAR(50),
    Hit_number INT
);

-- PRODUCTS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.PRODUCTS (
    PRODUCT_ID VARCHAR(50),
    PRICE FLOAT,
    NAME VARCHAR(100),
    INVENTORY NUMBER(38,0)
);

-- ADDRESSES
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.ADDRESSES (
    ADDRESS_ID VARCHAR(50),
    ZIPCODE NUMBER(38,0),
    COUNTRY VARCHAR(50),
    ADDRESS VARCHAR(150),
    STATE VARCHAR(50)
);

-- USERS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.USERS (
    USER_ID VARCHAR(50),
    UPDATED_AT TIMESTAMP_NTZ,
    ADDRESS_ID VARCHAR(50),
    LAST_NAME VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ,
    PHONE_NUMBER VARCHAR(20),
    FIRST_NAME VARCHAR(50),
    EMAIL VARCHAR(100)
);

-- ORDER_ITEMS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.ORDER_ITEMS (
    ORDER_ID VARCHAR(50),
    PRODUCT_ID VARCHAR(50),
    QUANTITY NUMBER(38,0)
);

-- PROMOS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.PROMOS (
    PROMO_ID VARCHAR(50),
    DISCOUNT FLOAT,
    STATUS VARCHAR(50)
);
```

Tenemos que ejecutar todos esos creates, una vez hecho vamos a echarle un ojo más de cerca a las diferencias entre estás tablas y las de BRONZE. 

En primer lugar podemos ver que ya no todos los tipos de dato son VARCHAR(256), ahora tenemos los tipos de datos ajustados al dato que nos viene, además hemos añadido algunos campos, HIT_NUMBER en EVENTS y DELIVERY_TIME_HOURS en ORDERS. Para rellenar estas tablas tendremos que tener en cuenta estas diferencias.

El primer paso es preparar un comando INSERT INTO que cargará el dato desde BRONZE a SILVER aplicando las transformaciones necesarias, os dejo aqui el ejemplo de PROMOS para que veáis que no tiene mucha ciencia.
```
-- PROMOS
INSERT INTO MY_DB.MY_SILVER_SCHEMA.PROMOS 
SELECT 
    PROMO_ID::varchar(50),
    DISCOUNT::float,
    STATUS::varchar(50)
FROM my_db.my_bronze_schema.promos;
```
Tened cuidado de sustituir las bases de datos y esquemas por los vuestros. 

Además, podéis ver que en este caso la única transformación que tenemos que hacer es cambiar el tipo de dato (CASTEOS), snowflake tiene una manera sencilla de aplicarlos que es con ::tipo_de_dato, esto habrá que hacerlo para todas las tablas. Os recomendamos empezar por las que no son EVENTS ni ORDERS ya que para estás habrá que añadir un campo calculado que tiene un poco más de dificultad.

Una vez hechos los INSERT INTO para las tablas sencillas podéis ejecutarlos y comprobar que efectivamente se rellenan las tablas de SILVER.

Tablas "complicadas":
Vamos a por ORDERS, el nuevo campo se llama DELIVERY_TIME_HOURS y os dicen desde negocio que este campo es la diferencia en horas desde que se crea un pedido hasta que se entrega. Tendreís que buscar una función de Snowflake que consiga hacer esto, tenéis el timestamp de creación en CREATED_AT y el de entrega en DELIVERED_AT.

Con EVENTS ocurre lo siguiente, a un data science flipado se le ha ocurrido la brillante idea de hacer un análisis de las sesiones en la web y nos ha pedido que le añadamos un campo que numere los distintos eventos dentro de una sesión. Osea que si dentro de una sesión lo primero que hace un usuario es visitar un producto, que ese registro lleve HIT_NUMBER = 1 , luego visita otro HIT_NUMBER = 2, luego va al carrito y revisa HIT_NUMBER = 3 y luego compra HIT_NUMBER = 4.
Esto se hace con una window function. Os dejamos intentarlo y pasaremos la solución por mattermost si se complica.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 4: CAPA GOLD

Ya tenemos nuestra SILVER que da gusto verla, nuestros campos están con los formatos adecuados y hemos añadido un par de campos que aportan una información muy interesante.

- Construir 2 o 3 agrupados en Gold. Estará bien que 2 fueran a través de procedimiento y el tercero fuera una vista que otro día les diremos que va lenta y que es necesario materializar.
