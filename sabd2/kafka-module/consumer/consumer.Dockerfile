FROM openjdk:11-jre-slim

# Imposta la directory di lavoro alla radice
WORKDIR /

# Definisce un build argument per il percorso del file JAR
ARG JAR_FILE

# Copiare il file JAR dell'applicazione Kafka utilizzando il build argument
COPY ${JAR_FILE} app.jar

# Creare una directory di output
RUN mkdir output

# Comando predefinito per eseguire l'applicazione
CMD ["java", "-cp", "app.jar", "com.sabd2.kafka.CSVConsumer", "kafka://kafka:9092"]
