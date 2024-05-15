# Clonado y Time travel

En el siguiente ejercicio aprenderemos a:

- Clonar una tabla y un esquema.
- Volver al estado anterior de una tabla gracias al *time travel* de Snowflake.

En esta práctica seguiremos utilizando los datos que ingestamos en este apartado:

[Cargando Datos](https://www.notion.so/Cargando-Datos-2e9dfac104ed47a68e39e23708a31ec2?pvs=21)

**Partes de la práctica:**

---

# 1. Clonados

## a) Clonado de tabla

Por ejemplo, si queremos clonar la tabla *Brands*  del esquema *Production* en una tabla con nombre, pongamos, *brands_clonado*, debemos ejecutar el siguiente comando:

```sql
USE DATABASE DEV_DEMO_BRZ_DB_<USER>;
USE SCHEMA PRODUCTION;
CREATE OR REPLACE TABLE brands_clonado CLONE brands; 
```

Al hacer el clonado podemos observar que ambas tablas son idénticas:

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/7e0dcde1-5a03-4ce7-a56f-1497c72c368f/180da063-36ef-472f-8a97-d39fb28a4712/Untitled.png)

## b) Clonado de esquema

De la misma forma, podemos clonar un esquema. Por ejemplo, podemos clonar el esquema *Production.*

```sql
CREATE OR REPLACE SCHEMA production_clonado CLONE PRODUCTION;
```

Usando este comando podemos observar que el esquema nuevo tiene la misma estructura que el original:

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/7e0dcde1-5a03-4ce7-a56f-1497c72c368f/2a15debb-065a-4475-b434-ef1274cba4a5/Untitled.png)

# 2. Time travel

## a) Time travel por tiempo

El Time Travel en Snowflake es una poderosa funcionalidad que permite a los usuarios acceder a datos históricos dentro de un marco de tiempo específico, lo cual es extremadamente útil para una variedad de casos de uso en el análisis de datos y la gestión de bases de datos. Aquí enseñaremos brevemente cómo podemos esta funcionalidad.

Utilizaremos el schema de “Production” y la tabla de “Brands” para hacer las pruebas:

```sql
use schema production;
-- Vemos los datos de nuestra tabla
select * from brands;
-- Insertamos valores en la tabla
insert into brands values(10,'Marca',null, CURRENT_TIMESTAMP());
-- Vemos los datos de nuestra tabla
select * from brands;
```

Ahora, borraremos el valor que le acabamos de insertar y observaremos mediante Time travel cómo podemos ver la versión inicial de la tabla:

```sql
-- Borramos el valor nuevo añadido
delete from brands where brand_id=10;
-- Vemos los datos de nuestra tabla
select * from brands;
-- Observamos cómo era la tabla hace 2 minutos:
select * from brands at (offset => -60*2);
-- También se puede hacer de esta forma, la cual le resta dos minutos al tiempo actual:
select * from brands at (TIMESTAMP => TIMESTAMPADD(minute, -2, CURRENT_TIMESTAMP()));
```

Utilizando el time travel podemos ver cómo era la tabla hace dos minutos, en la cual no habíamos borrado el registro cuyo brand_id era el 10. Esta versión antigua se puede guardar con el siguiente comando:

```sql
-- Para guardar esta versión en otra tabla
create or replace table brands_clone CLONE brands at (TIMESTAMP => TIMESTAMPADD(minute, -2, CURRENT_TIMESTAMP()));
```

*Nota: es posible que hayan pasado los dos minutos y la tabla vuelva a estar como está en la actualidad. Podemos volver a la versión anterior añadiendo más minutos a la función de TIMESTAMPADD(minute, -2, CURRENT_TIMESTAMP()) por ejemplo, si queremos que vuelva a la de hace 5 minutos, la función será TIMESTAMPADD(minute, -5, CURRENT_TIMESTAMP()).*

Siguiendo con el ejercicio, podemos borrar la tabla y recuperarla también con el time travel de Snowflake:

```sql
-- Borramos la tabla
drop table brands;
-- Vemos que nuestra tabla está borrada
select * from brands;
-- Recuperamos la tabla borrada
undrop table brands;
-- Vemos que nuestra tabla ha vuelto
select * from brands;
-- Insertamos de nuevo el valor brand_id=10
insert into brands values(10,'Marca',null, CURRENT_TIMESTAMP());
-- Y podemos seguir usando las versiones anteriores de la tabla.
-- Como se puede ver, no aparece el nuevo registro que acabamos de insertar
-- si hago el time travel a hace 5 minutos:
select * from brands at (TIMESTAMP => TIMESTAMPADD(minute, -5, CURRENT_TIMESTAMP()));

```

## a) Time travel por id de la consulta

También podemos utilizar el time travel con el id de la consulta. 

En ese caso:
