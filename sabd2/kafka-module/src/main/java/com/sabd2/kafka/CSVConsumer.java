package com.sabd2.kafka;
import com.opencsv.CSVWriter;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class CSVConsumer {
    private static final Logger logger = Logger.getLogger(CSVConsumer.class.getName());

    public static void main(String[] args) {

        logger.info("Avvio del consumer Kafka");
        String kafkaBootstrapServers = System.getenv("INTERNAL_KAFKA_ADDR");//"kafka://kafka:9092";
        String outputDirectory = "/output"; // definito come variabile locale


        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            if (outputDir.mkdirs()) {
                System.out.println("Directory di output creata: " + outputDirectory);
            } else {
                System.err.println("Errore nella creazione della directory di output: " + outputDirectory);
                return;
            }
        } else if (!outputDir.isDirectory() || !outputDir.canWrite()) {
            System.err.println("La directory di output non è scrivibile: " + outputDirectory);
            return;
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        consumerProperties.put("group.id", "consumer");
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Pattern.compile("^(query).*$"));

            HeaderManager headerManager = new HeaderManager(outputDirectory); // Gestore delle intestazioni

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    headerManager.writeMessageToCSV(record.topic(), record.value());
                }
                // commit asincrono degli offset
                consumer.commitAsync();
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Kafka consumer", e);
        }
    }

    // gestore delle intestazioni e della scrittura su file
    static class HeaderManager {
        private final String outputDirectory;
        private final Set<String> topicsWithHeaderWritten;

        HeaderManager(String outputDirectory) {
            this.outputDirectory = outputDirectory;
            this.topicsWithHeaderWritten = new HashSet<>();
        }


        void writeMessageToCSV(String topic, String message) {
            String csvFileName = outputDirectory + "/" + topic + ".csv";

            try (ICSVWriter writer = new CSVWriterBuilder(new FileWriter(csvFileName, true))
                    .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER) // No quote character
                    .build()) {
                // scrivi l'intestazione se il file è stato appena creato
                if (!topicsWithHeaderWritten.contains(topic)) {
                    String[] header = getHeaderForTopic(topic);
                    if (header != null) {
                        writer.writeNext(header);
                    }
                    topicsWithHeaderWritten.add(topic);
                }

                // rimuovi virgolette e parentesi dal messaggio
                message = message.replaceAll("[()]", "").replace("\"", "");
                // scrivi il messaggio nel file CSV
                writer.writeNext(message.split(","));
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed to write record to file", e);
            }
        }

        private String[] getHeaderForTopic(String topic) {
            System.out.println(topic);
            if (topic.startsWith("query1")) {
                return new String[]{"ts", "vault_id", "count", "mean_s194", "stddev_s194"};
            } else if (topic.startsWith("query2")) {
                return new String[]{"ts", "vault_id1", "failures1","disk_details [model,serial]",
                        "vault_id2", "failures2","disk_details [model,serial]",
                        "vault_id3", "failures3","disk_details [model,serial]",
                        "vault_id4", "failures4","disk_details [model,serial]",
                        "vault_id5", "failures5","disk_details [model,serial]",
                        "vault_id6", "failures6","disk_details [model,serial]",
                        "vault_id7", "failures7", "disk_details [model,serial]",
                        "vault_id8", "failures8","disk_details [model,serial]",
                        "vault_id9", "failures9","disk_details [model,serial]",
                        "vault_id10", "failures10","disk_details [model,serial]"};
            } else if (topic.startsWith("query3")) {
                return new String[]{"ts", "vault_id1", "count", "min", "25perc", "50perc", "75perc", "max"};
            }
            return null;
        }
    }
}
