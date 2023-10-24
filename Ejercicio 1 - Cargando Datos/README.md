
# Carga de datos

Bueno, ahora sí que sí, ha llegado el momento. Desde hace dos horas, una llamada del equipo de desarrollo ha provocado que tu equipo de datos parezca un avispero. Os acaban de comunicar que por fin, los datos de usuarios, productos, pedidos, etc... están disponibles para ser consumidos.

Eso si, para variar, no han sido muy claros en que datos vais a recibir ni tampoco su estructura. Os han dejado unos ficheros de muestra con datos, los cuales negocio está ansioso por poder visualizar y testear a través de la herramienta de BI.

Tu primera tarea ya está aquí.

Lo primero será crear las tablas donde cargar los datos [CREATE TABLES].

Ahora deberás aprovisionar las tablas de orders, events, addresses, order_items y users a partir de los ficheros que hay en el stage @bronze_stage.

Cuando crees que ya está todo, te das cuenta que todavía faltan por cargar las tablas de products y la tabla de promos... Por suerte, tienes los ficheros en tu local [FICHEROS].

Tendrás que ingeniártelas para cargar crear un stage en tu base de datos, cargar los ficheros y posteriormente volcar sus datos en las tablas correspondientes.

Y finalmente... lo conseguiste. Ya puedes respirar !!!


