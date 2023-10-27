# Orquestación y Monitorización

El Team Lead de tu equipo ha validado tus procedimientos de transformación y te felicita. Ahora te pide un paso más, automatizar toda esta parte del proceso, por supuesto de forma incremental.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASOS PREVIOS: 

Para empezar vamos a realizar una serie de tareas previas a la creación de nuestro Stream y nuestras Tasks.

En primer lugar y para asegurarnos que todos partimos de la misma base, vamos a clonar en nuestro esquema particular:
```
USE DATABASE MY_DB;
    
CREATE OR REPLACE SCHEMA BRONZE CLONE CURSO_SNOWFLAKE_DE_2023.BRONZE;
CREATE OR REPLACE SCHEMA SILVER CLONE CURSO_SNOWFLAKE_DE_2023.SILVER;
CREATE OR REPLACE SCHEMA GOLD CLONE CURSO_SNOWFLAKE_DE_2023.GOLD;
```

Ahora crearemos en nuestra Base de Datos y esquema GOLD la siguiente tabla que hace un agregado del número de pedidos y su status por fecha, para ello ejecutamos:

Recuerda reemplazar siempre MY_DB por el nombre de tu Base de Datos:
```
CREATE OR REPLACE TABLE MY_DB.GOLD.ORDERS_STATUS_DATE AS (
    SELECT 
        TO_DATE(CREATED_AT) AS FECHA_CREACION_PEDIDO,
        STATUS,
        COUNT(DISTINCT ORDER_ID) AS NUM_PEDIDOS
    FROM 
        MY_DB.SILVER.ORDERS 
    GROUP BY    
        TO_DATE(CREATED_AT),
        STATUS
    ORDER BY 1 DESC
);
```

¿Y si mejor encapsulamos la creación de esta tabla en un procedimiento almacenado como aprendimos ayer?, así podremos recrearla cada vez que la información se actualice:

```
CREATE OR REPLACE PROCEDURE MY_DB.GOLD.update_gold_orders_status()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN

    CREATE OR REPLACE TABLE MY_DB.GOLD.ORDERS_STATUS_DATE AS (
        SELECT 
            TO_DATE(CREATED_AT) AS FECHA_CREACION_PEDIDO,
            STATUS,
            COUNT(DISTINCT ORDER_ID) AS NUM_PEDIDOS
        FROM 
            MY_DB.SILVER.ORDERS
        GROUP BY    
            TO_DATE(CREATED_AT),
            STATUS
        ORDER BY 1 DESC
    );

    return 'Tabla ORDERS_STATUS_DATE actualizada con éxito';
END;
```

Hasta aquí los pasos previos, ahora vamos con lo nuevo!

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 1: STREAM

En primer lugar vamos a crear el stream (tipo APPEND_ONLY) sobre la tabla ORDERS_HIST, pero ojo!, lo vamos a crear sobre la tabla ORDERS_HIST que está en la base de datos común! (CURSO_SNOWFLAKE_DE_2023.BRONZE.ORDERS_HIST), no la que tienes en tu propia Base de Datos.

Lo haremos así para simular la entrada de nuevos pedidos, es decir, una vez esté todo configurado insertaremos nosotros a modo de prueba registros y si todo está bien configurado, el pipeline que estais a punto de construir funcionará a la perfección!.

Recordad ejecutad siempre desde vuestra Base de Datos y crearemos el stream con el nombre ORDERS_STREAM.

Será un Stream sobre la tabla:
```
CURSO_SNOWFLAKE_DE_2023.BRONZE.ORDERS_HIST;
```

Como siempre, la documentación es nuestra amiga:

https://docs.snowflake.com/en/sql-reference/sql/create-stream

Todo bien? Si lanzáis SHOW STREAMS lo véis?

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 2: TAREA RAIZ (ROOT TASK)

El segundo paso será constuir un pequeño árbol de tareas y de esta forma construiremos nuestro pipeline automatizado.

Para ello sugerimos que la tarea raiz (ROOT_TASK) se desencadene cuando el stream reciba información (SYSTEM$STREAM_HAS_DATA('ORDERS_STREAM')), dicho de otro modo, cuando os generemos un pedido y podemos programarlo (scheduler) para que haga esta comprobación cada minuto.

Así pues, vamos a la documentación a resfrescar este create task:

https://docs.snowflake.com/en/sql-reference/sql/create-task

¿Y que va a ejecutar esta task?, buena pregunta...pues vamos a actualizar nuestra tabla de Orders de Silver. Importante que sea la de vuestra Base de Datos y Esquema!

Para ello os proponemos que completéis la consulta Merge que haría esta operación, completad tanto el update si machea, como el insert si no machea:

Documentación del Merge:

https://docs.snowflake.com/en/sql-reference/sql/merge

```
--MERGE
    MERGE INTO SILVER.ORDERS t
    USING 
    (
        SELECT *
        FROM
            MY_DB.BRONZE.ORDERS_STREAM 
    ) s ON t.ORDER_ID = s.ORDER_ID
            WHEN MATCHED THEN UPDATE ...
```

Para que no perdáis tiempo con los casteos que hicísteis ayer en la tabla de Orders...os los dejamos por aquí:
```
ORDER_ID::varchar(50),
SHIPPING_SERVICE::varchar(20),
replace(SHIPPING_COST,',','.')::decimal,
ADDRESS_ID::varchar(50),
CREATED_AT::timestamp_ntz,
IFNULL(promo_id,'N/A'),
ESTIMATED_DELIVERY_AT::timestamp_ntz,
(replace(ORDER_COST,',','.'))::decimal,
USER_ID::varchar(50),(replace(s.ORDER_TOTAL,',','.'))::decimal,
DELIVERED_AT::timestamp_ntz,
TRACKING_ID::varchar(50),
STATUS::varchar(20),
TIMESTAMPDIFF(HOUR,created_at,delivered_at)
```

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 3: TAREA HIJA

Para probar el concepto que vimos en la teoría de tarea hija en el árbol de tareas, crearéis una nueva tarea (TASK_HIJA) que se lanzará justo cuando la tarea padre (ROOT_TASK) finalice.

Para ello recordad que en la documentación para la creación de tareas existe la clausula AFTER y a continuación el nombre de la tarea predecesora.

https://docs.snowflake.com/en/sql-reference/sql/create-task

En este caso la tarea hija ejecutará el procedimiento almacenado que hemos creado en las tareas previas:
update_gold_orders_status()


-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 4: ACTIVA TUS TAREAS

Una vez creadas las tareas, no olvides activarlas, para ello:
```
ALTER TASK IF EXISTS MY_SCHEMA.ROOT_TASK RESUME;
ALTER TASK IF EXISTS MY_SCHEMA.TASK_HIJA RESUME;
```

Para comprobar si la tarea raíz (ROOT_TASK) está comprobando cada minuto, que es el tiempo que le configuramos, si el stream tiene datos o no podemos lanzar una consulta en el task history y comprobarlo:

```
--CHECK TASK HISTORY
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = 'ROOT_TASK';
```

El campo STATE aparecerá como SCHEDUELD y tras cada revisión, como todavía no hemos insertado datos en el ORDERS_HIST, pasará a estado SKIPPED hasta que insertemos datos y el debería ser SUCCEEDED (ojalá) o FAILED (si algo hay mal).

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 5: COMPROBACIÓN

Ahora vamos a insertar valores en la tabla de la base de datos común a la que apunta el STREAM que has creado, como si estuvieran entrando y actualizándose pedidos y si todo va bien, los SPs de las tareas no fallan y la magia del SQL hace su función, deberás ver como se insertan en la tabla Orders de tu esquema Silver y como la tabla de Gold va mostrando los cambios.
```
SELECT * FROM GOLD.ORDERS_STATUS_DATE;
```
Si has llegado hasta aquí...enhorabuena!! has aprendido una parte importante y muy útil para la ingesta y transformación de la info gracias a la versatilidad que las Streams+Tasks nos aportan en Snowflake.


