# Tablas dinámicas

Las tablas dinámicas son una función relativamente nueva de Snowflake diseñada para **actualizar automáticamente los conjuntos de datos derivados a medida que cambian los datos subyacentes.** Estas tablas están pensadas para reducir la sobrecarga de mantenimiento de los conjuntos de datos que necesitan mantenerse actualizados con los datos base de los que derivan.

¿Dónde habíamos visto esto? Cuando estuvimos viendo los Task, también hacíamos actualizaciones de los datos de las tablas. Entonces, ¿cuándo utilizo una tarea y cuando una tabla dinámica?:

- Las Task son más adecuadas para escenarios en los que se necesita un control sobre cuándo y cómo se ejecutan los procesos de datos, especialmente para operaciones son continuas basadas en intervalos.
- Las tablas dinámicas son perfectas para cuando necesitamos que un conjunto de datos siempre se encuentre en su forma más actualizada, sin necesidad de activar manualmente dichas actualizaciones.


# 🔔 Nueva tarea

¡Sorpresa! Aunque todo parecía estar listo y en funcionamiento, siempre hay cambios por hacer. Esto refleja una realidad muy importante en el manejo de datos: los flujos de datos **NUNCA SE TERMINAN**.

Vamos a ver las tareas que se necesitan realizar utilizando Snowflake y las tablas dinámicas:

Tu equipo ha oído hablar de las tablas dinámicas y espera que utilices estas tablas dinámicas para gestionar todo el flujo de datos, desde la captura inicial (Bronze) hasta las transformaciones intermedias (Silver) y finalmente la refinación hacia los datos de alta calidad (Gold).

## 1. ¿Cómo está montado actualmente nuestro flujo de datos?

### a) Capa Silver

Este proceso actualmente está implementado mediante un procedimiento almacenado, pero ahora queremos cambiarlo por tablas dinámicas. 

```sql
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
    FROM curso_snowflake_de_2023.bronze.orders_hist;
    
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

```

## **b) Capa Gold**

Dentro de la capa Gold tenemos dos procedimientos que utilizamos para que precalculen datos específicos y nosotros podemos usar estos datos en cuadros de mando:

1. **Procedimiento para detalles de sesión de Gold:** agrupamos los datos de eventos en la tabla Silver, para cada sesión identificada por **`session_id`** , indicándose si la sesión resultó en un pedido o no, el máximo número de interacciones dentro de la sesión y la diferencia en minutos entre el primer y el último evento registrado en la sesión.

```sql
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
```

2. **Procedimiento para Análisis del Estado en Gold:** realiza un análisis por estado sobre los datos de órdenes y direcciones de la capa Silver, utilizando una agrupación por estado, una suma del costo de pedidos por estado y un conteo de pedidos y usuarios únicos por estado.

```sql
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

## 2. Tu tarea de crear tablas dinámicas con el flujo de datos antiguo

### a) Capa Silver

En esta etapa, debes crear 7 tablas dinámicas para procesar de manera automática e incremental los datos que llegan a las tablas de bronze de la base de datos centralizada (**curso_snowflake_de_2024**). Utilizarás el warehouse WH_BASICO y deberás configurar un retraso de procesamiento (lag) de 1 minuto.

Coge las tablas que tenemos actualmente en Silver y modifícalas para que sean tablas dinámicas.

Os dejamos un atajo a la documentación https://docs.snowflake.com/en/user-guide/dynamic-tables-tasks-create

### b) Capa Gold

Finalmente, la etapa Gold también necesita de tablas dinámicas. Estas tablas se actualizarán en respuesta a cambios en las tablas de Silver, utilizando un "lag" configurado como DOWNSTREAM. 

Coge las tablas que tenemos actualmente en Gold y modifícalas para que sean tablas dinámicas.

## 3. Tarea extra

¡Tu equipo no te deja que descanses 🥵! El negocio tiene claro el dato que le gustaría consultar en un cuadro de mando, y para el que tú deberías de preparar dos agrupados que ya precalculen esta información. Cada uno de ellos también serán una tabla dinámica. En este caso, el lag deberemos configurarlo como DOWNSTREAM, de esta forma estas tablas dinámicas se actualizarán cuando lo hagan las de Silver. 

1. Suma de Shipping_Cost agrupada por Código Postal: Deberás crear una tabla dinámica que realice este cálculo.
2. Suma de Order_Cost por Nombre de Producto dónde el estado de la orden sea "shipped”: Igualmente, deberás crear una tabla dinámica para este cálculo.

</br>
</br>

# Snowpark

Snowpark proporciona una biblioteca intuitiva para consultar y procesar datos a escala en Snowflake. Mediante una biblioteca, puedes crear aplicaciones que procesen datos en Snowflake **sin mover los datos al sistema donde se ejecuta el código de su aplicación**.

Snowflake proporciona actualmente bibliotecas Snowpark para tres lenguajes: **Java, Python y Scala**.

En este caso, vamos a poner un ejemplo con Python, y ¿por qué con Python? Porque Snowflake nos permite utilizar un Worksheet con Python directamente, sin tener que instalar nada y desde la propia interfaz de Snowflake:

![Untitled (4)](https://github.com/javipo84/Curso_Snowflake/assets/166698078/d1ffc4b4-a805-422a-a23c-584492db42e1)


# **1. Configuración inicial**

Vamos a hacer un clone de nuestra tabla de orders para poder modificarla sin problema:

```sql
CREATE OR REPLACE TABLE BASE_DE_DATOS_ALUMNO.BRONZE.ORDERS_COPY 
  CLONE BASE_DE_DATOS_ALUMNO.BRONZE.ORDERS;
```

# **2. Creación y visualización de un dataframe**

Primero, crearemos un dataframe simple desde una tabla existente en tu base de datos y veremos cómo mostrar los resultados.

```sql
import snowflake.snowpark as snowpark

def main(session: snowpark.Session): 
    # Seleccionamos la tabla desde tu esquema y base de datos
    df = session.table("BASE_DE_DATOS_ALUMNO.BRONZE.ORDERS_COPY")
    return df
```

Para mostrar los resultados, podemos verlos también desde la terminal. Esto se hará seleccionando que la salida sea un String y usando la función print de python:

![Untitled (3)](https://github.com/javipo84/Curso_Snowflake/assets/166698078/8d92bd53-3489-4117-99b9-c8cbff135b11)


```sql
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    # Creamos un dataframe
    df = session.table("ORDERS_COPY")
    print("df.show(10) resultado")
    print(df.show(10))
    print('')
    print('___________________________')
    print('___________________________')
    print("df.count() resultado")
    print(df.count())
    return df
```

Para que nos vuelva a salir como tabla en la pestaña de “Results” tenemos que volver a selecciona en Settings>Return type>`Table()` :

![Untitled (2)](https://github.com/javipo84/Curso_Snowflake/assets/166698078/18976eb7-97df-45cb-98a1-c75cbd3e5144)


# 3. Transformación de los datos

Con Python podemos escoger las columnas que queremos consultar dentro del dataframe creado:

```sql
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Seleccionamos algunas columnas de interés
    df = df.select("ORDER_ID", "CUSTOMER_ID", "ORDER_STATUS", "ORDER_DATE").limit(10)
    return df
```

## a) Filtrado de filas

Podemos filtrar por filas:

```sql
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as f

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Filtramos las órdenes con estado 4
    df = df.filter(f.col("ORDER_STATUS") == 4).limit(10)
    return df

```

## **b) Inserción de nuevas columnas**

También podemos agregar una columna que indique el número de días que tardó el envío desde la fecha de pedido hasta la fecha de envío.

```sql
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as f

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Calculamos los días entre el pedido y el envío
    df = df.with_column("SHIPPING_DELAY", f.datediff("SHIPPED_DATE", "ORDER_DATE"))
    return df

```

## c) Agregación de datos

Nuestros datos también pueden ser agrupados como si estuviésemos haciendo un `GROUP BY` en SQL pero en este caso con Python:

```sql
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as f

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Agrupamos por STORE_ID y realizamos varias agregaciones en ORDER_TOTAL
    grouped_data = df.groupBy("STORE_ID").agg(
        f.sum("ORDER_TOTAL").as_("TOTAL_SALES"),
        f.avg("ORDER_TOTAL").as_("AVERAGE_SALES"),
        f.min("ORDER_TOTAL").as_("MIN_SALES"),
        f.max("ORDER_TOTAL").as_("MAX_SALES"),
        f.count("*").as_("COUNT_ORDERS")
    )

    return grouped_data
```

## d) **Uniones entre tablas**

Supongamos que queremos unir la tabla **`orders`** con **`customers`** donde queremos mantener todos los registros de **`orders`** y solo los datos coincidentes de **`customers`**.

```sql
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df_orders = session.table("ORDERS_COPY")
    df_customers = session.table("customers")
    # Realizamos un left join
    left_join_data = df_orders.join(df_customers, df_orders["CUSTOMER_ID"] == df_customers["CUSTOMER_ID"], "left")
    
    return left_join_data
```

## **e) Ejercicio: creación de una columna de correo electrónico para el cliente**

Generemos una dirección de correo electrónico ficticia para el cliente utilizando su **`CUSTOMER_ID`**.

<details>
<summary>Solución:</summary>
<br>
```sql import snowflake.snowpark as snowpark
        import snowflake.snowpark.functions as f
        from snowflake.snowpark.types import StringType

    def main(session: snowpark.Session):
        session.use_database('BASE_DE_DATOS_ALUMNO')
        session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Creamos una dirección de email ficticia para el cliente
    df = df.with_column('CUSTOMER_EMAIL', f.concat(f.cast(f.col("CUSTOMER_ID"), StringType()), f.lit('@example.com')))
    return df ```
</details>
