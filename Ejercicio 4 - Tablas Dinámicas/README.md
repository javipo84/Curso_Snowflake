# Tablas Dinámicas

!!!Sorpresa!!! Cuando todo estaba listo y funcionando, hay que realizar modificaciones. Esta es la características más importante de los flujos de datos, **NUNCA SE TERMINAN**.

Estas son las tareas que piden realizar.

## Tablas Dinámicas

Por un lado Snowflake ha habilitado una nueva funcionalidad, llamada Dynamic Table, la cual puede facilitar el proceso de transformación y orquestación del dato. Tu equipo te pide utilizar estos objetos para realizar todo el pipeline de datos (Bronze --> Silver --> Gold). 


### Creación de Tablas Dinámicas en Silver

En esta capa, deberás crear 7 tablas dinámicas para ingestar de forma automática e incremental el dato que va llegando a las tablas bronze de la base de datos centralizada (**curso_snowflake_de_2023**). Deberás usar el warehouse WH_BASICO y configurar un lag de 1 minuto.

Recuerda, que esto ya lo hiciste en su momento a través de un procedimiento almacenado.

```
-- PROCEDURES

--- CARGAR SILVER
CREATE OR REPLACE PROCEDURE MYDB.SILVER.INSERT_PROCEDURE_SILVER()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
BEGIN

    -- ORDERS
    INSERT INTO MYDB.SILVER.ORDERS 
    SELECT 
        ORDER_ID::varchar(50),
        SHIPPING_SERVICE::varchar(20),
        (replace(SHIPPING_COST,',','.'))::decimal,
        ADDRESS_ID::varchar(50),
        CREATED_AT::timestamp_ntz,
        IFNULL(promo_id,'N/A'),
        ESTIMATED_DELIVERY_AT::timestamp_ntz,
        (replace(ORDER_COST,',','.'))::decimal,
        USER_ID::varchar(50),
        (replace(ORDER_TOTAL,',','.'))::decimal,
        DELIVERED_AT::timestamp_ntz,
        TRACKING_ID::varchar(50),
        STATUS::varchar(20),
        TIMESTAMPDIFF(HOUR,created_at,delivered_at)
    FROM MYDB.bronze.orders;
    
    -- EVENTS
    INSERT INTO MYDB.SILVER.EVENTS 
    SELECT 
        EVENT_ID::varchar(50),
        PAGE_URL::varchar(200),
        EVENT_TYPE::varchar(50),
        USER_ID::varchar(50),
        PRODUCT_ID::varchar(50),
        SESSION_ID::varchar(50),
        CREATED_AT::timestamp_ntz,
        ORDER_ID::varchar(50),
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at ASC)
    FROM MYDB.bronze.events;
    
    -- PRODUCTS
    INSERT INTO MYDB.SILVER.PRODUCTS
    SELECT 
        PRODUCT_ID::Varchar(50),
        (replace(PRICE,',','.'))::decimal,
        NAME::varchar(100),
        INVENTORY::number(38,0)
    FROM MYDB.bronze.products_original;
    
    -- ADDRESSES
    INSERT INTO MYDB.SILVER.ADDRESSES 
    SELECT 
        ADDRESS_ID::varchar(50),
        ZIPCODE::number(38,0),
        COUNTRY::varchar(50),
        ADDRESS::varchar(150),
        STATE::varchar(50)
    FROM MYDB.bronze.addresses;
    
    -- USERS
    INSERT INTO MYDB.SILVER.USERS 
    SELECT 
        USER_ID::varchar(50),
        UPDATED_AT::timestamp_ntz,
        ADDRESS_ID::varchar(50),    
        LAST_NAME::varchar(50),
        CREATED_AT::timestamp_ntz,
        PHONE_NUMBER::varchar(20),
        FIRST_NAME::varchar(50),
        EMAIL::varchar(100)
    FROM MYDB.bronze.users;
    
    -- ORDER_ITEMS
    INSERT INTO MYDB.SILVER.ORDER_ITEMS 
    SELECT 
        ORDER_ID::varchar(50),
        PRODUCT_ID::varchar(50),
        QUANTITY::number(38,0)
    FROM MYDB.bronze.order_items;
    
    -- PROMOS
    INSERT INTO MYDB.SILVER.PROMOS 
    SELECT 
        PROMO_ID::varchar(50),
        DISCOUNT::float,
        STATUS::varchar(50)
    FROM MYDB.bronze.promos;    

    RETURN 'Insertado con éxito máquina';
END;



---- GOLD SESSION DETAILS
CREATE OR REPLACE PROCEDURE MYDB.GOLD.INSERT_GOLD_SESSION_DETAILS()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
BEGIN
    
    -- SESSION_DETAILS
    INSERT INTO MYDB.GOLD.session_details 
    SELECT 
        session_id,
        CASE WHEN MAX(order_id) IS NULL THEN FALSE ELSE TRUE END,
        MAX(hit_number),
        TIMESTAMPDIFF(MINUTE,MIN(created_at),MAX(created_at))
    FROM MYDB.SILVER.events 
    GROUP BY session_id;
    
    RETURN 'Insertado con éxito máquina';
END;


--- GOLD STATE ANALYSIS

CREATE OR REPLACE PROCEDURE MYDB.GOLD.INSERT_GOLD_STATE_ANALYSIS()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
BEGIN
    -- GENERAL_STATE_ANALYSIS
    INSERT INTO MYDB.GOLD.GENERAL_STATE_ANALYSIS 
    SELECT 
        a.state,
        SUM(o.order_cost),
        COUNT(*),
        COUNT(DISTINCT o.user_id),
        ROUND(SUM(o.shipping_cost)/SUM(o.order_total),2),
        mode(shipping_service)
    FROM MYDB.SILVER.ORDERS o 
    LEFT JOIN MYDB.SILVER.ADDRESSES a ON o.address_id = a.address_id 
    GROUP BY a.state;
    RETURN 'Insertado con éxito máquina';
END;  
```

Por lo tanto, ya tienes las consultas para pasar los datos desde esta base de datos al esquema silver de TU base de datos. Cada uno de estas consultas debería de ser una tabla dinámica. Por ejemplo DT_ODERS, ....

### Creación de Agrupados en Gold

Por último, finalmente negocio tiene claro el dato que le gustaría consultar en un cuadro de mando, y para el que tú deberías de preparar dos agrupados que ya precalculen esta información. Cada uno de ellos también serán una tabla dinámica. En este caso, el lag deberemos configurarlo como DOWNSTREAM, de esta forma estas tablas dinámicas se actualizarán cuando lo hagan las de Silver. 

#### 1. Total de Shipping_Cost por Código Postal

Deberás crear una tabla dinámica que realice este cálculo.

#### 2. Total de Order_Cost por Producto dónde el estado de la orden sea "shipped"

Igualmente, deberás crear una tabla dinámica para este cálculo.



## Diagrama Pipeline

![image](https://github.com/javipo84/Curso_Snowflake/assets/51535157/fbc772a6-cd78-46c3-9f29-d7ac3e5e6838)





