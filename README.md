# Práctica de Spark Streaming

Realizada por Iván Penedo.

## Instrucciones de ejecución

Para poder ejecutar el proyecto entregado, es necesario tener en ejecución los servidores de Kafka y ZooKepper. De no ser así, pueden levantarse desde el directorio `/opt/Kafka/kafka_2.11-2.3.0/` con los comandos

```bash
sudo bin/zookeeper-server-start.sh config/zookeeper.properties
sudo bin/kafka-server-start.sh config/server.properties
```

Si no se tienen los topics de Kafka `purchases`, `facturas_erroneas`, `cancelaciones`, `anomalias_kmeans` y `anomalias_bisect_kmeans` creados, pueden crearse con

```bash
bin/kafka-topics.sh --create bin/kafka-topics.sh --topic TOPIC_NAME --partitions 1 -replication-factor 1 --zookeeper localhost:2181
```

reemplazando `TOPIC_NAME` con el nombre del topic que se quiera creear.

Para poder compilar la aplicación, es necesario ejecutar la instrucción

```bash
sbt clean assembly
```

Dentro del proyecto se incluyen los ficheros del entrenamiento de los modelos `KMeans` y `BisectingKMeans`, pero si se quieran reentrenar, podría ejecutarse el script `start_training.sh`.

La aplicación desarrollada requiere que llegen nuevas trasnacciones por el topic de `purchases`, por lo que se ha de ejecutar el script `productiondata.sh` para su correcto funcionamiento.

Tras inicializar todos los requisitos necesrios, se puede ejecutar la aplciación con el siguiente comando:

```bash
./start_pipeline.sh
```

Si se quiere comprobar el correcto funcionamiento de este, además de comprobar que los mensajes llegar por el topic indicado, se pueden consumir los topics utilizando la siguiente instrucción desde `/opt/Kafka/kafka_2.11-2.3.0/`

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic TOPIC_NAME -from-beginning
```

reemplazando `TOPIC_NAME` por cualquiera de los topics.
