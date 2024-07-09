FROM openjdk:11-jre-slim

# Installare le dipendenze necessarie
RUN apt-get update && apt-get install -y \
    wget \
    tar \
    gzip \
    && apt-get clean

# Creare la directory per l'applicazione Kafka e impostare la directory di lavoro
RUN mkdir -p /opt/kafka-app
WORKDIR /opt/kafka-app

# Copiare il file JAR dell'applicazione Kafka
COPY ./target/kafka-module-1.0-SNAPSHOT.jar .

# Scaricare il dataset dal URL
RUN wget http://www.ce.uniroma2.it/courses/sabd2324/project/hdd-smart-data_medium-utv.tar.gz

# Estrarre il file tar.gz
RUN tar -xzf hdd-smart-data_medium-utv.tar.gz --strip 1

# Impostare il comando per eseguire l'applicazione
#CMD ["java", "-cp", "kafka-1.0-SNAPSHOT-jar-with-dependencies.jar", "HddSmartDataProducer"]
CMD ["java", "-cp", "kafka-module-1.0-SNAPSHOT.jar","com.sabd2.kafka.HddSmartDataProducer", "raw_data_medium-utv_sorted.csv", "kafka://kafka:9092", "5000000"]
