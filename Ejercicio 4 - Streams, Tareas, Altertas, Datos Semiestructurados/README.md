# Orquestación y Monitorización

El Team Lead de tu equipo ha validado tus procedimientos de transformación y te felicita. Ahora te pide un paso más, automatizar toda esta parte del proceso, por supuesto de forma incremental.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASOS PREVIOS: 

Para empezar vamos a realizar una serie de tareas previas a la creación de nuestro Stream y nuestras Tasks.

En primer lugar y para asegurarnos que todos partimos de la misma base, vamos a clonar en nuestro esquema particular:
```
USE DATABASE MY_DB;
    
CREATE OR REPLACE SCHEMA BRONZE CLONE CURSO_DATA_ENGINEERING_2024.BRONZE;
CREATE OR REPLACE SCHEMA SILVER CLONE CURSO_DATA_ENGINEERING_2024.SILVER;
CREATE OR REPLACE SCHEMA GOLD CLONE CURSO_DATA_ENGINEERING_2024.GOLD;
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

En primer lugar vamos a crear el stream (tipo APPEND_ONLY) sobre la tabla ORDERS_HIST, pero ojo!, lo vamos a crear sobre la tabla ORDERS_HIST que está en la base de datos común! (CURSO_DATA_ENGINEERING_2024.BRONZE.ORDERS_HIST), no la que tienes en tu propia Base de Datos.

Lo haremos así para simular la entrada de nuevos pedidos, es decir, una vez esté todo configurado insertaremos nosotros a modo de prueba registros y si todo está bien configurado, el pipeline que estais a punto de construir funcionará a la perfección!.

Recordad ejecutad siempre desde vuestra Base de Datos y crearemos el stream con el nombre ORDERS_STREAM.

Será un Stream sobre la tabla:
```
CURSO_DATA_ENGINEERING_2024.BRONZE.ORDERS_HIST;
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

--------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------------------------------

# PRÁCTICA DATOS SEMIESTRUCTURADOS
Una vez que hemos entendido cómo funcionan los datos semi-estructurados, vamos a aprender a usarlos en Snowflake. ¿Listos? ¡Vamos!

En esta práctica vamos a utilizar dos archivos json:
- Simple.json
```
{
    "resultados": [
        {
            "nivel": "101",
            "asignatura": "Francés 1",
            "departamento": "Idiomas"
        },
        {
            "nivel": "111",
            "asignatura": "Trigonometría",
            "departamento": "Matemáticas"
        },
        {
            "nivel": "110",
            "asignatura": "Geometría",
            "departamento": "Matemáticas"
        },
        {
            "nivel": "102",
            "asignatura": "Álgebra 2",
            "departamento": "Matemáticas"
        },
        {
            "nivel": "101",
            "asignatura": "Álgebra 1",
            "departamento": "Matemáticas"
        },
        {
            "nivel": "110",
            "asignatura": "Física",
            "departamento": "Ciencia"
        },
        {
            "nivel": "105",
            "asignatura": "Biología",
            "departamento": "Ciencia"
        },
        {
            "nivel": "101",
            "asignatura": "Química",
            "departamento": "Ciencia"
        }
    ]
}
```
- Anidado.json
```
{
    "resultados": [
        {
            "nivel": "101",
            "asignatura": "Francés 1",
            "departamento": "Idiomas",
            "profesor": {
                "nombre": "Claire Dubois",
                "email": "cdubois@instituto.edu"
            },
            "libro_de_texto": {
                "titulo": "Parlons Français",
                "autor": "Marc Lavoine",
                "edicion": "5ta"
            }
        },
        {
            "nivel": "111",
            "asignatura": "Trigonometría",
            "departamento": "Matemáticas",
            "profesor": {
                "nombre": "Carlos Gutiérrez",
                "email": "cgutierrez@instituto.edu"
            },
            "libro_de_texto": {
                "titulo": "Fundamentos de Trigonometría",
                "autor": "Isabel Sánchez",
                "edicion": "3ra"
            }
        },
        {
            "nivel": "110",
            "asignatura": "Geometría",
            "departamento": "Matemáticas",
            "profesor": {
                "nombre": "Laura Jiménez",
                "email": "ljimenez@instituto.edu"
            },
            "libro_de_texto": {
                "titulo": "Geometría Descriptiva",
                "autor": "Juan Perez",
                "edicion": "7ma"
            }
        },
        {
            "nivel": "102",
            "asignatura": "Álgebra 2",
            "departamento": "Matemáticas",
            "profesor": {
                "nombre": "Enrique Salazar",
                "email": "esalazar@instituto.edu"
            },
            "libro_de_texto": {
                "titulo": "Álgebra Intermedia",
                "autor": "Ana María López",
                "edicion": "2da"
            }
        },
        {
            "nivel": "101",
            "asignatura": "Álgebra 1",
            "departamento": "Matemáticas",
            "profesor": {
                "nombre": "José Martín",
                "email": "jmartin@instituto.edu"
            },
            "libro_de_texto": {
                "titulo": "Principios de Álgebra",
                "autor": "Luis Rodríguez",
                "edicion": "4ta"
            }
        },
        {
            "nivel": "110",
            "asignatura": "Física",
            "departamento": "Ciencia",
            "profesor": {
                "nombre": "Marta Vidal",
                "email": "mvidal@instituto.edu"
            },
            "libro_de_texto": {
                "titulo": "Física para Estudiantes",
                "autor": "Diana Rivas",
                "edicion": "6ta"
            }
        },
        {
            "nivel": "105",
            "asignatura": "Biología",
            "departamento": "Ciencia",
            "profesor": {
                "nombre": "Antonio Banderas",
                "email": "abanderas@instituto.edu"
            },
            "libro_de_texto": {
                "titulo": "La Vida en la Tierra",
                "autor": "Patricia Clark",
                "edicion": "8va"
            }
        },
        {
            "nivel": "101",
            "asignatura": "Química",
            "departamento": "Ciencia",
            "profesor": {
                "nombre": "Ricardo Montalbán",
                "email": "rmontalban@instituto.edu"
            },
            "libro_de_texto": {
                "titulo": "Química General",
                "autor": "Mario Casas",
                "edicion": "9na"
            }
        }
    ]
}
```
Primero creamos un schema llamado SEMIESTRUCTURADOS dentro de nuestra base de datos de bronze:
```
USE DATABASE ALUMNOX_BRONZE_DB;
CREATE SCHEMA SEMIESTRUCTURADOS;
USE SCHEMA SEMIESTRUCTURADOS;
```
Para cargar los datos dentro de este schema, vamos a hacerlo mediante la interfaz para hacerlo más rápido:

![Untitled (22)](https://github.com/javipo84/Curso_Snowflake/assets/156344357/5af376d6-ad7d-4ed4-be49-97d50c94a993)

Utilizando ‘Browse’ seleccionamos el fichero de simple.json y en la parte de abajo, le indicamos el nombre que queremos que tenga esta tabla. En este caso, le pondremos ‘SIMPLE’:
![Untitled (23)](https://github.com/javipo84/Curso_Snowflake/assets/156344357/4d2c707a-7e8c-4017-b501-62446977041b)

Ahora seleccionaremos que el DATA TYPE sea un VARIANT:
Le damos a Load y ya tendríamos nuestro fichero SIMPLE. Hacemos lo mismo con el otro fichero llamado anidado.json y le damos a la tabla el nombre de ANIDADO. 

Recuerda ponerlo como tipo VARIANT para poder hace el ejercicio. Si te has olvidado de ponérselo, no pasa nada, borra la tabla y vuélvela a crear (DROP TABLE ANIDADO; y ¡listo! vuelta a empezar).

Si hacemos un SELECT de la tabla veremos que nuestros datos ya están cargados:
```
SELECT * FROM SIMPLE;
```
![Untitled (24)](https://github.com/javipo84/Curso_Snowflake/assets/156344357/082c53f8-3b9a-4396-bbd4-c0b1d08167a4)

```
SELECT * FROM ANIDADO;
```
![Untitled (25)](https://github.com/javipo84/Curso_Snowflake/assets/156344357/4f34eb35-3ca2-40d6-9349-9e065c70bc72)

## Trabajar con datos semiestructurados
Ahora que ya tenemos cargadas las tablas, vamos a hacer un par de ejercicios simples para ver nuestros datos.

### 1. Ver nuestros datos
#### a) simple.json
Hemos visto que ya tenemos nuestros datos cargados en las tablas, pero no se pueden ver muy bien porque se quedan con el formato json. Si queremos ver los datos un poquito mejor separándolos por columnas podemos usar:

```
SELECT
  value:asignatura::STRING AS asignatura,
  value:departamento::STRING AS departamento,
  value:nivel::STRING AS nivel
FROM simple,
LATERAL FLATTEN(input => simple.resultados);
```

La función **`[FLATTEN`**](https://docs.snowflake.com/en/sql-reference/functions/flatten) de Snowflake toma un campo semiestructurado y produce una nueva fila para cada elemento si el campo es un array, o para cada campo si es un objeto.

**`LATERAL FLATTEN(input => simple.resultados)`**: Esta parte de la consulta utiliza la función **`FLATTEN`** junto con un **`JOIN`** lateral para expandir los datos anidados dentro del JSON.

- **`input => simple.resultados`** especifica que la entrada para la función **`FLATTEN`** es el campo **`resultados`** encontrado en el JSON que se encuentra dentro de la tabla **`simple`**.
- **`LATERAL`** permite que cada fila generada por **`FLATTEN`** se una lateralmente con el resultado de tabla **`simple`.**

#### b) anidado.json
Al igual que hemos hecho con simple, podemos hacerlo con anidado. No obstante, ahora en este anidado, dentro de resultados tenemos profesor y dentro de este nombre y email y al igual pasa con libro de texto. Esto lo solucionaremos de la siguiente manera:

```
SELECT
  value:asignatura::STRING AS asignatura,
  value:departamento::STRING AS departamento,
  value:nivel::STRING AS nivel,
  value:profesor.nombre::STRING AS nombre_profesor,
  value:profesor.email::STRING AS email_profesor,
  value:libro_de_texto.titulo::STRING AS titulo_libro,
  value:libro_de_texto.autor::STRING AS autor_libro,
  value:libro_de_texto.edicion::STRING AS edicion_libro
FROM ANIDADO,
LATERAL FLATTEN(input => anidado.resultados);
```

### 2. Ejercicios prácticos

1. Obtén todas las asignaturas que pertenecen al departamento de "Matemáticas".
```
SELECT
  value:asignatura::STRING AS asignatura
FROM simple,
LATERAL FLATTEN(input => simple.resultados)
WHERE value:departamento::STRING = 'Matemáticas';
```
2. ¿Cuántas asignaturas hay en cada nivel educativo? Ordena de manera ascendente por nivel educativo la consulta.
```
SELECT
  value:nivel::STRING AS nivel,
  COUNT(*) AS cantidad_asignaturas
FROM simple,
LATERAL FLATTEN(input => simple.resultados)
GROUP BY value:nivel::STRING
ORDER BY value:nivel::STRING;
```

3. Identifica todas las asignaturas que usan libros de texto en su "5ta" edición.
```
SELECT
  value:asignatura::STRING AS asignatura,
  value:departamento::STRING AS departamento,
  value:libro_de_texto.titulo::STRING AS titulo_libro,
  value:libro_de_texto.autor::STRING AS autor_libro,
  value:libro_de_texto.edicion::STRING AS edicion_libro
FROM anidado,
LATERAL FLATTEN(input => anidado.resultados)
WHERE value:libro_de_texto.edicion::STRING = '5ta';
```

