# PySpark-Streaming
________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/d140574e-3713-412f-8988-797d90f9b9aa)  ![image](https://github.com/user-attachments/assets/451f8a4e-7f94-44f4-a77f-b1686747dfaa)  ![image](https://github.com/user-attachments/assets/e65def2d-56f8-46a1-8bd6-0b40737f5d84)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🎯 Descripción del Proyecto
________________________________________________________________________________________________________________________________________________________________________________________________________________

La transición del procesamiento por lotes (Batch) al procesamiento en tiempo real (Streaming) no es solo cambiar la API de read a readStream. Introduce un paradigma completamente nuevo: manejar el desorden inherente de los datos en movimiento.

Este laboratorio documenta mi dominio de Apache Spark Structured Streaming, alejándome de los ejemplos básicos para implementar conceptos avanzados de ingeniería de datos: gestión del tiempo de evento vs. tiempo de procesamiento, manejo de datos atrasados (late data), y cómo agrupar eventos lógicos en el tiempo sin alterar la estructura de los micro-lotes.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## ⏱️ El Problema del Streaming: Event-Time vs. Processing-Time
________________________________________________________________________________________________________________________________________________________________________________________________________________

En un pipeline Batch, los datos son estáticos. En Streaming, los datos son una corriente continua donde los eventos llegan desordenados.

 * **Event-Time:** Cuándo ocurrió el evento en el mundo real (ej. el usuario hizo clic a las 12:00:01).
 * **Processing-Time:** Cuándo Spark leyó el evento (ej. a las 12:00:05 debido a un micro-lote o congestión de red).Comprender esta diferencia es el prerrequisito absoluto para implementar políticas de retención y ventanas de tiempo.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🪟 Arquitectura de Ventanas (Windowing)
________________________________________________________________________________________________________________________________________________________________________________________________________________

Agrupar eventos continuos requiere definir fronteras temporales. Implementé y analizé las tres estrategias nativas de Spark:

* **Tumbling Windows (Ventanas Fijas sin solapamiento):** Ejecuta el cálculo cada X minutos sobre datos no superpuestos. Útil para métricas de popularidad por intervalos exactos (ej. "Ventas de los últimos 5 minutos").

* **Sliding Windows (Ventanas Deslizantes):** Igual que Tumbling, pero con solapamiento. Útil para promedios móviles suavizados (ej. "Promedio de ventas de los últimos 5 minutos, calculado cada 1 minuto").

* **Session Windows (Ventanas de Sesión):** Agrupa eventos separados por un periodo de inactividad (ej. "Una sesión de usuario termina si no hay actividad por 30 minutos").

________________________________________________________________________________________________________________________________________________________________________________________________________________
🛡️ Watermarks: La Solución al "Late Data" (Dato Atrasado)
________________________________________________________________________________________________________________________________________________________________________________________________________________

Este es el concepto más complejo y valorado en Streaming. Si un evento del minuto 12:00 llega tarde a las 12:05, ¿lo ignoramos o lo procesamos?

Implementé Watermarks para definir el umbral de tardanza máxima permitida. Spark usa esto internamente para rastrear el estado y descartar duplicados en micro-lotes posteriores, garantizando semánticas "Exactly-Once" en la agregación.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 💻 Implementación Práctica en Databricks (Arquitectura del Código)
________________________________________________________________________________________________________________________________________________________________________________________________________________

1.	Creamos una cuenta en Databricks Free Edition.
   
2.	Creamos un workspace llamdo stream.
   
3.	Creamos un volumen llamado streaming.

![image](https://github.com/user-attachments/assets/ecaf68c7-f2af-4b9c-bfa9-d5faa091fe9e)

4.	Ahora creamos una carpeta la cual llamaremos Spark_Streaming, y luego abrimos nuestro editor de código de preferencia, en mi caso VSC y, vinculamos la carpeta Spark_Streaming jalando al editor de código, y ahí creamos tres archivos con los siguientes códigos y nombres.

4.1	archivo 1, llamado day1.json.

código: 

        {
          "order_id": "ORD1001",
          "timestamp": "2025-06-01T10:15:00Z",
          "customer": {
            "customer_id": 501,
            "name": "John Doe",
            "email": "john@example.com",
            "address": {
              "city": "Toronto",
              "postal_code": "M5H 2N2",
              "country": "Canada"
            }
          },
          "items": [
            {
              "item_id": "I100",
              "product_name": "Wireless Mouse",
              "quantity": 2,
              "price": 25.99
            },
            {
              "item_id": "I101",
              "product_name": "USB-C Adapter",
              "quantity": 1,
              "price": 15.49
            }
          ],
          "payment": {
            "method": "Credit Card",
            "transaction_id": "TXN7890"
          },
          "metadata": [
            {"key": "campaign", "value": "back_to_school"},
            {"key": "channel", "value": "email"}
          ]
        }

        
4.2	archivo 2, llamado day2.json.

código:

        {
          "order_id": "ORD1002",
          "timestamp": "2025-06-01T10:30:00Z",
          "customer": {
            "customer_id": 502,
            "name": "Alice Smith",
            "email": "alice@example.com",
            "address": {
              "city": "Vancouver",
              "postal_code": "V5K 0A1",
              "country": "Canada"
            }
          },
          "items": [
            {
              "item_id": "I102",
              "product_name": "Bluetooth Keyboard",
              "quantity": 1,
              "price": 45.00
            }
          ],
          "payment": {
            "method": "PayPal",
            "transaction_id": "TXN7891"
          },
          "metadata": [
            {"key": "campaign", "value": "cyber_monday"},
            {"key": "channel", "value": "affiliate"}
          ]
        }

4.3	archivo 3, llamado day3.json.

código:

        {
          "order_id": "ORD1003",
          "timestamp": "2025-06-01T11:00:00Z",
          "customer": {
            "customer_id": 503,
            "name": "David Lee",
            "email": "david@example.com",
            "address": {
              "city": "Calgary",
              "postal_code": "T2P 1G1",
              "country": "Canada"
            }
          },
          "items": [
            {
              "item_id": "I103",
              "product_name": "Noise Cancelling Headphones",
              "quantity": 1,
              "price": 199.99
            },
            {
              "item_id": "I104",
              "product_name": "Laptop Stand",
              "quantity": 1,
              "price": 29.99
            },
            {
              "item_id": "I105",
              "product_name": "HDMI Cable",
              "quantity": 2,
              "price": 10.00
            }
          ],
          "payment": {
            "method": "Debit Card",
            "transaction_id": "TXN7892"
          },
          "metadata": [
            {"key": "referrer", "value": "instagram"},
            {"key": "coupon", "value": "WELCOME10"}
          ]
        }


5.	Cargamos el volumen day1.json a Databricks.

![image](https://github.com/user-attachments/assets/bfd8c420-cbc5-47d9-abd1-11f3e309f0f9)

6.	Ahora vamos a workspace y creamos una nueva carpeta de trabajo llamada ApacheSparkStreaming.

![image](https://github.com/user-attachments/assets/3518d103-7bbb-47e0-92e5-784685a253ca)

![image](https://github.com/user-attachments/assets/5c90aad0-885b-4ca0-ad8d-74fed440624a)

7.	Luego creamos un notebook.
   
![image](https://github.com/user-attachments/assets/014d502c-cf46-4be7-b188-b9e81c1ad9f0)

![image](https://github.com/user-attachments/assets/9f80772f-e39c-4457-909d-986c49b8802b)

Ahora ingresamos el siguiente código.

Código: 

        df = spark.read.format("json")\
            .option("inferSchema", True)\
            .option("multiline", True)\
            .load("/Volumes/workspace/stream/streaming/jsonsource/day1.json")
        display(df)


![image](https://github.com/user-attachments/assets/07330bac-7a34-4c1a-ba22-b0a468782503)

![image](https://github.com/user-attachments/assets/34fb67ff-f657-42b1-a958-3daf09759017)

![image](https://github.com/user-attachments/assets/58215074-e874-476b-9126-0f184ea43512)

•	Ahora seguiremos haciendo el método del aplanado para seguir transformando la parte de información del cliente que se encuentra en forma de clave : valor de json  a la tabla.

Código json:

             {
               "order_id": "ORD1001",
               "timestamp": "2025-06-01T10:15:00Z",
                "customer": {
                "customer_id": 501,
                "name": "John Doe",
                "email": "john@example.com",
                "address": {
                  "city": "Toronto",
                  "postal_code": "M5H 2N2",
                  "country": "Canada"
                }
              },

![image](https://github.com/user-attachments/assets/f878d057-1ef0-4a6e-b82c-3f307bb35b48)

Código:

        df.select("order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country").display()


![image](https://github.com/user-attachments/assets/4ec3620a-b8ba-4275-ba4f-5124f1ebea11)

![image](https://github.com/user-attachments/assets/de15a0c3-19e9-423d-85e0-45924e059352)

•	Ahora para el tratamiento o aplanado de los ítems que se encuentran en forma de diccionario dentro de un array del archivo day1.json. En este caso usaremos el método de explode_outer( ).

Parte del código:

                  "items": [
                      {
                        "item_id": "I100",
                        "product_name": "Wireless Mouse",
                        "quantity": 2,
                        "price": 25.99
                      },
                      {
                        "item_id": "I101",
                        "product_name": "USB-C Adapter",
                        "quantity": 1,
                        "price": 15.49
                      }
                    ],


Primero importamos las librerías al inicio.


![image](https://github.com/user-attachments/assets/9026f5c9-76ce-4780-84aa-f9de37fcbc98)


Código:

        df = df.select("items", "order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country")

        df = df.withColumn("items", explode_outer("items"))

        display(df)

![image](https://github.com/user-attachments/assets/8708edb9-1fb9-478a-aeea-99d645dffe90)

Código:

        df = df.select("items.item_id", "items.price", "items.product_name", "items.quantity", "order_id", "timestamp", "customer_id", "name", "email", "city", "postal_code", "country" )
        display(df)

![image](https://github.com/user-attachments/assets/64c0d2ee-d82d-4ff3-a90e-26d7b57a2163)

•	Ahora aplanaremos el payment.

Código del archivo day1.json parte payment.

Código:

        "payment": {
            "method": "Credit Card",
            "transaction_id": "TXN7890"
          },
          "metadata": [
            {"key": "campaign", "value": "back_to_school"},
            {"key": "channel", "value": "email"}
          ]
        }


Primero en el código anterior aumentamos al final del código “payment”, “metadata”.

Así:

     df = df.select("items", "order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country", "payment","metadata")

     df = df.withColumn("items", explode_outer("items"))

     display(df)


•	Y luego en la siguiente celda de Databricks escribimos el siguiente código.

Código:

        df = df.select("items.item_id", "items.price", "items.product_name", "items.quantity", "order_id", "timestamp", "customer_id", "name", "email", "city", "postal_code", "country", "payment.method", "payment.transaction_id", "metadata")
        display(df)


y ejecutamos con play.


![image](https://github.com/user-attachments/assets/e821bf4c-eece-46b7-a2df-0a7da87600a3)


•	Ahora tenemos que visualizar los 2 diccionarios dentro del array metadata, ejecutando el siguiente código.

Código:

        df = df.withColumn("metadata", explode_outer("metadata"))
        display(df)

![image](https://github.com/user-attachments/assets/e4c7c18a-5d5e-4198-8d8d-901de518f51b)

•	Ahora ordenamos los datos y eliminamos las columnas que estarían demás, pasando el siguiente código al dataframe anterior.

Código actualizado:

                    df = df.withColumn("metadata", explode_outer("metadata"))
                    df = df.select("*", "metadata.key", "metadata.value").drop("metadata")
                    display(df)


y ahora veremos los datos aplanados correctamente.


![image](https://github.com/user-attachments/assets/7e97fff0-dc2a-4f7d-8e05-7901c7ca0b04)

•	READ STREAMING DATA

Ahora creamos la estructura completa.

Código:

        my_schema = """order_id STRING,
                timestamp STRING,
                customer STRUCT<
                    customer_id: INT,
                    name: STRING,
                    email: STRING,
                    address: STRUCT< 
                        city: STRING,
                        postal_code: STRING,
                        country: STRING
                    >
                >,
                items ARRAY<STRUCT<
                    item_id: STRING,
                    product_name: STRING,
                    quantity: INT,
                    price: DOUBLE
                >,
                payment STRUCT<
                    method: STRING,
                    transaction_id: STRING
                >,
                metadata ARRAY<STRUCT<
                    key: STRING,
                    value: STRING
                >>"""

Código:

        df = spark.readStream.format("json")\
                .option("multiline", True)\
                .load("/Volumes/workspace/stream/streaming/jsonsource")

        # Transformations

        df = df.select("items", "order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country", "payment","metadata")

        df = df.select("items.item_id", "items.price", "items.product_name", "items.quantity", "order_id", "timestamp", "customer_id", "name", "email", "city", "postal_code", "country", "payment.method", "payment.transaction_id", "metadata")

        df = df.withColumn("metadata", explode_outer("metadata"))
        df = df.select("*", "metadata.key", "metadata.value").drop("metadata")


Código:

        df.writeStream.format("delta")\
                .outputMode("append")\
                .trigger(once=True)\
                .option("path", "/Volumes/workspace/stream/streaming/jsonsink/Data")\
                .option("checkpointLocation", "/Volumes/workspace/stream/streaming/jsonsink/checkpoint")\
                .start()

![image](https://github.com/user-attachments/assets/114e5bfc-2643-40bb-9ce0-aa2b82c768c8)


![image](https://github.com/user-attachments/assets/d3ce1aa9-75b0-4186-af8d-4e424c5e8f22)

![image](https://github.com/user-attachments/assets/95fa0ed0-bfda-488c-88a0-1da7b5e568a9)


Archiving

Primero creamos un archivo llamado jsonsourcenew, directo desde el editor de código de Databricks con el siguiente comando.

Código:

        dbutils.fs.mkdirs("/Volumes/workspace/stream/streaming/jsonsourcenew")


![image](https://github.com/user-attachments/assets/d14d5183-d753-40d3-a664-74189c570630)

Y para verificar si se creó vamos a catálogo.

![image](https://github.com/user-attachments/assets/1b8b9636-43d1-49c6-a41e-be9f77f3e442)

Luego creamos otro archivo llamado jsonsourcearchive

Código:

       dbutils.fs.mkdirs("/Volumes/workspace/stream/streaming/jsonsourcearchive")


![image](https://github.com/user-attachments/assets/dc63e047-b90a-40fb-92f3-bdede089f8ef)

![image](https://github.com/user-attachments/assets/3a2630fb-e766-426e-b6b5-b87052cfee68)


Ahora replicamos el esquema anterior.

Código:

        my_schema = """order_id STRING,
                 timestamp STRING,
               customer STRUCT<
                    customer_id: INT,
                    name: STRING,
                    email: STRING,
                    address: STRUCT< 
                        city: STRING,
                        postal_code: STRING,
                        country: STRING
                    >
                >,
                items ARRAY<STRUCT<
                    item_id: STRING,
                    product_name: STRING,
                    quantity: INT,
                    price: DOUBLE
                >>,
                payment STRUCT<
                    method: STRING,
                    transaction_id: STRING
                >,
                metadata ARRAY<STRUCT<
                    key: STRING,
                    value: STRING
                >>"""


![image](https://github.com/user-attachments/assets/f158f11e-f1c0-4795-bfa0-82df5cab6dce)


Luego los siguientes códigos.

Codigo:

        df = spark.readStream.format("json")\
                    .option("multiline", True)\
                    .schema(my_schema)\
                    .option("cleanSource", "archive")\
                    .option("sourceArchiveDir", "/Volumes/workspace/stream/streaming/jsonsourcearchive")\
                    .load("/Volumes/workspace/stream/streaming/jsonsourcenew")

        # Transformations

        df = df.select("items", "order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country", "payment","metadata")

        df = df.select("items.item_id", "items.price", "items.product_name", "items.quantity", "order_id", "timestamp", "customer_id", "name", "email", "city", "postal_code", "country", "payment.method", "payment.transaction_id", "metadata")

        df = df.withColumn("metadata", explode_outer("metadata"))
        df = df.select("*", "metadata.key", "metadata.value").drop("metadata")

![image](https://github.com/user-attachments/assets/075349ac-df13-4c1c-a047-0ad32f65d181)

Código:

        df.writeStream.format("delta")\
                .outputMode("append")\
                .trigger(once=True)\
                .option("path", "/Volumes/workspace/stream/streaming/jsonsinknew/Data")\
                .option("checkpointLocation", "/Volumes/workspace/stream/streaming/jsonsinknew/checkpoint")\
                .start()

![image](https://github.com/user-attachments/assets/64c89118-20b7-409c-aebf-5fb8cf6f904b)

Código:
              
        SELECT * FROM delta.`/Volumes/workspace/stream/streaming/jsonsink/Data`



![image](https://github.com/user-attachments/assets/1d9f3335-cac7-4abe-ad6c-6c22a3cddb3d)


### OUTPUT MODES

Creamos una tabla con el siguiente código en las celdas de Databricks.

Código:

        CREATE TABLE workspace.stream.sourcetable
        (
            color STRING
        )


**USING DELTA**

Código:

        from delta.tables import DeltaTable 

![image](https://github.com/user-attachments/assets/23b93549-e735-4836-bec5-ba86dcf6c509)


código:

        DeltaTable.createIfNotExists(spark) \
            .tableName("workspace.stream.sinktable") \
            .addColumn("color", "STRING") \
            .execute()

![image](https://github.com/user-attachments/assets/7a71784c-2d03-4541-9263-99d910c06124)

Código:
        INSERT INTO workspace.stream.sourcetable VALUES 
        ('red'),
        ('green'),
        ('blue'),
        (‘yellow’),
        (‘orange’),
        (‘orange’)

![image](https://github.com/user-attachments/assets/34030246-b001-409f-87b1-4cf5324ef7c6)


Código:

        SELECT * FROM workspace.stream.sourcetable

![image](https://github.com/user-attachments/assets/cc80c3cb-bf47-44f4-bb83-96babea97e09)

Código:

        df = spark.readStream.table('workspace.stream.sourcetable') 


![image](https://github.com/user-attachments/assets/2bf18536-aa1d-4c76-81b3-1b0a9766747a)


Código:

        from pyspark.sql.functions import *

![image](https://github.com/user-attachments/assets/1e0914da-c9ee-4cbc-99f3-a3890163fe28)


Código:

        df = df.groupBy('color').agg(count("*").alias("count"))


![image](https://github.com/user-attachments/assets/4caa039d-d03d-4d86-bb2b-094e90b751a7)

Código:

        df.writeStream.format('delta')\
                    .outputMode('complete')\
                    .trigger(availableNow=True)\
                    .option("checkpointLocation", "/Volumes/workspace/stream/streaming/output/check")\
                    .option("path", "/Volumes/workspace/stream/streaming/output/Data")\
                    .start()


![image](https://github.com/user-attachments/assets/b13d3796-2c46-42d4-8c9b-6e65bef1fb9a)

Ahora volvemos a celda numero 4 y agragamos un verde, un azul y un rojo mas y volvemos a correr las celdas y ejecutamos el siguiente código.

Código:

        SELECT * FROM delta.`/Volumes/workspace/stream/streaming/output/Data`


![image](https://github.com/user-attachments/assets/cccf79b3-9e7e-4b9c-ad6f-668e5212e396)

**FOR EACH BATCH**

Creamos otra notebook llamada For Each Batch y creamos las siguientes celdas con sus respectivos códigos

Código:

        from pyspark.sql.functions import *


![image](https://github.com/user-attachments/assets/b3571042-a871-4814-8b16-805bfc83f7a7)

Código:

        df = spark.readStream.table('workspace.stream.sourcetable') 

![image](https://github.com/user-attachments/assets/1416b859-214f-4f42-8822-d4b1e5cd58ea)

Código:

        def myfunc(df, batch_id):

            df = df.groupBy('color').agg(count('*').alias('count'))

            # destination 1
            df.write.format('delta')\
                    .mode('append')\
                    .option("path", "/Volumes/workspace/stream/streaming/foreachsink/dest1")\
                    .save()

            # Destination 2
            df.write.format('delta')\
                    .outputMode('append')\
                    .option("path", "/Volumes/workspace/stream/streaming/foreachsink/dest2")\
                    .save()


![image](https://github.com/user-attachments/assets/11c1e285-40a2-4983-b6bc-ff7688f82047)

Ahora en volumes/streaming creamos archivo llamado foreachsink.

![image](https://github.com/user-attachments/assets/79f53e34-048d-4abb-be5d-3a5d57d80a09)

Ahora creamos las carpetas destino 1 y destino 2.

![image](https://github.com/user-attachments/assets/a62fedda-f91a-4104-b9f4-bee53ed584f6)

![image](https://github.com/user-attachments/assets/975af513-261a-4066-bb43-3679175891bd)

![image](https://github.com/user-attachments/assets/de40d8a5-affd-4d3f-9d0e-5eb73423f175)


Luego creamos otro archivo llamado checkpoint.

![image](https://github.com/user-attachments/assets/90878d98-6fed-445b-952c-40a87439928a)


Código:

        df.writeStream.foreachBatch(myfunc)\
            .outputMode("append")\
            .trigger(availableNow=True)\
            .option("checkpointLocation", "/Volumes/workspace/stream/streaming/foreachsink/checkpoint")\
            .start()


![image](https://github.com/user-attachments/assets/bd429687-9017-4438-81ca-2f9f9c54f6a2)

### WINDOW FUNCTIONS

**Sliding Windows (Ventanas Deslizantes)**

Creamos un nuevo notebook y pasamos los siguientes códigos en las celdas.

Codigo:

       CREATE TABLE workspace.stream.windowtbl
        (
            color STRING,
            event_date TIMESTAMP
        )

Código:

        INSERT INTO workspace.stream.windowtbl
        VALUES
        ('red', '2026-06-07T19:32:00.000+00:00'),
        ('green', '2026-06-07T19:32:00.000+00:00') 

Código:

        df = spark.readStream.table('workspace.stream.windowtbl') 


Ahora importamos la librería functions

código:

        from pyspark.sql.functions import *


código:

         df = df.groupBy('color', window('event_date', '10 minutes'))\
             .agg(count(lit(1)).alias('color_count'))


Código:

        df.writeStream.format("delta")\
            .outputMode("complete")\
            .trigger(availableNow=True)\
            .option("path", "/Volumes/workspace/stream/streaming/windows/Data")\
            .option("checkpointLocation", "/Volumes/workspace/stream/streaming/windows/checkpoint")\
            .start()

Código:

        SELECT * FROM delta.`/Volumes/workspace/stream/streaming/windows/Data`

![image](https://github.com/user-attachments/assets/b6ba3de8-2690-4cf7-b846-eccd4b672339)

Ahora intentamos agregar dos nuevos registros con diferente hora en la misma celda 2 editando los códigos anteriores.

Código:

        ('green', '2026-06-07T19:42:00.000+00:00'),
        ('green', '2026-06-07T19:52:00.000+00:00') 

![image](https://github.com/user-attachments/assets/b90894bc-c328-4d46-889f-69f522796b5a)

Ahora ejecutamos todas las celdas anteriores y vamos a ver los resultados en la celda numero 7, donde creará una ventana.

![image](https://github.com/user-attachments/assets/fbbf134f-90a4-4aa6-86f9-921558d266e7)


**Session Windows (Ventanas de Sesión)**

En esta sesión trabajaremos con la misma estructura de código anterior para ver los cambios.

Ahora en la celda 2 cambiaremos el código.

Código:

        INSERT INTO workspace.stream.windowtbl
        VALUES
        ('red', '2026-06-07T19:53:00.000+00:00') 


Y veremos lo siguiente:

![image](https://github.com/user-attachments/assets/27e4d31c-8465-4c2d-a1c9-393bb734a86e)


![image](https://github.com/user-attachments/assets/cb0fb5af-0857-4452-99be-9bc7a9853940)

