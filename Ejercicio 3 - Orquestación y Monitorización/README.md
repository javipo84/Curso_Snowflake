# Orquestación y Monitorización

El Team Lead de tu equipo ha validado tus procedimientos de transformación y te felicita. Ahora te pide un paso más, automatizar toda esta parte del proceso, por supuesto de forma incremental.

Crear tareas, Stream y ejemplo de Alerta.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 1: STREAM

En primer lugar vamos a crear el stream (tipo standard) sobre la tabla Orders, pero ojo!, lo vamos a crear sobre la tabla Orders que está en la base de datos común [nombre de la bbdd/esquema/Orders] !, no la que tienes en tu propia Base de Datos.

Lo haremos así para simular la entrada de nuevos pedidos, es decir, una vez esté todo configurado insertaremos nosotros a modo de prueba registros y si todo está bien configurado, el pipeline que estais a punto de construir funcionará a la perfección!.

Como siempre, la documentación es nuestra amiga:

https://docs.snowflake.com/en/sql-reference/sql/create-stream


-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 2: TAREA RAIZ (ROOT TASK)

El primer paso será encapsular los distintos procedimientos almacenados que hicisteis en la sesión de ayer en un árbol de tareas y de esta forma construiremos nuestro pipeline automatizado.

Para ello sugerimos que la tarea raiz (ROOT_TASK) se desencadene cuando el stream reciba información (SYSTEM$STREAM_HAS_DATA('MYSTREAM')), dicho de otro modo, cuando os generemos un pedido y podemos programarlo (scheduler) para que haga esta comprobación cada minuto.

Así pues, vamos a la documentación a resfrescar este create task:

https://docs.snowflake.com/en/sql-reference/sql/create-task

¿Y que va a ejecutar esta task?, buena pregunta...pues vamos a actualizar vuestra tabla de Orders. Importante que sea la de vuestra Base de Datos y Esquema!

Para ello y como no somos tan malos, os facilitamos la consulta Merge que haría esta operación:

MERGE...


-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 3: TAREA HIJA

Para probar el concepto que vimos en la teoría de tarea hija en el árbol de tareas, crearéis una nueva tarea (TASK_HIJA) que se lanzará justo cuando la tarea padre (ROOT_TASK) finalice.

Para ello recordad que en la documentación para la creación de tareas existe la clausula AFTER y a continuación el nombre de la tarea predecesora.

https://docs.snowflake.com/en/sql-reference/sql/create-task

En este caso la tarea hija ejecutará el siguiente código:


-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 4: ACTIVA TUS TAREAS

Una vez creadas las tareas, no olvides activarlas, para ello:

ALTER TASK IF EXISTS MY_SCHEMA.ROOT_TASK RESUME;
ALTER TASK IF EXISTS MY_SCHEMA.TASK_HIJA RESUME;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 5: COMPROBACIÓN

Ahora vamos a insertar valores en la tabla de la base de datos común a la que apunta el STREAM que has creado, como si estuvieran entrando pedidos y si todo va bien, los SPs de las tareas no fallan y la mágia del SQL hace su función, deberás ver como se insertan en la tabla Orders de tu Base de Datos / Esquema y como (lo que el otro sp haga).

Si has llegado hasta aquí...enhorabuena!! has aprendido una parte importante y muy útil para la ingesta y transformación de la info gracias a la versatilidad que las Streams+Tasks nos aportan en Snowflake.


