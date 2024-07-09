package com.sabd2.kafka;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.logging.Logger;

public class HddSmartDataProducer {

    public static final Logger logger = Logger.getLogger(HddSmartDataProducer.class.getSimpleName());
    private static volatile boolean keepRunning = true;
    public static void main(String[] args) throws InterruptedException, IOException {

        int speedup=500000000;

        String kafkaUrl = System.getenv("INTERNAL_KAFKA_ADDR");
        String topic = System.getenv("TOPIC");


        String csvFile = "raw_data_medium-utv_sorted.csv";

        String datasetUrl = "http://www.ce.uniroma2.it/courses/sabd2324/project/hdd-smart-data_medium-utv.tar.gz";
        String localTarGz = "hdd-smart-data_medium-utv.tar.gz";
        String localCsv = "raw_data_medium-utv_sorted.csv";

        if (!new File(csvFile).exists()) {
            System.out.println("The dataset doesn't exist locally. Downloading and extracting it from " + datasetUrl);
            // scaricare il dataset
            downloadFile(datasetUrl, localTarGz);

            // estrarre il file tar.gz
            extractTarGz(localTarGz, localCsv);

            // righe da aggiungere cause watermarks
            String[] newRows = {
                    "2023-04-24T00:00:00.000000,0,0,1,1120,154524840.0,,0.0,23.0,0.0,533028348.0,,66090.0,0.0,23.0,,,0.0,0.0,0.0,0.0,23.0,0.0,0.0,7664.0,23.0,,,0.0,0.0,0.0,,,,,,65983.0,70908545888.0,494415929912.0",
                    "2023-04-24T00:00:00.000000,0,0,1,1000,154524840.0,,0.0,23.0,0.0,533028348.0,,66090.0,0.0,23.0,,,0.0,0.0,0.0,0.0,23.0,0.0,0.0,7664.0,23.0,,,0.0,0.0,0.0,,,,,,65983.0,70908545888.0,494415929912.0",
                    "2023-04-25T00:00:00.000000,0,0,1,1000,154524840.0,,0.0,23.0,0.0,533028348.0,,66090.0,0.0,23.0,,,0.0,0.0,0.0,0.0,23.0,0.0,0.0,7664.0,23.0,,,0.0,0.0,0.0,,,,,,65983.0,70908545888.0,494415929912.0",
                    "2023-04-25T00:00:00.000000,0,0,1,1120,154524840.0,,0.0,23.0,0.0,533028348.0,,66090.0,0.0,23.0,,,0.0,0.0,0.0,0.0,23.0,0.0,0.0,7664.0,23.0,,,0.0,0.0,0.0,,,,,,65983.0,70908545888.0,494415929912.0"
            };


            try (BufferedReader reader = new BufferedReader(new FileReader(localCsv));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(localCsv, true))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    //leggi
                }

                // aggiungi le nuove righe
                for (String newRow : newRows) {
                    writer.newLine();
                    writer.write(newRow);
                }

                System.out.println("Righe aggiunte con successo al file CSV.");
            } catch (IOException e) {
                e.printStackTrace();
            }


            csvFile = localCsv;
        }

        // configure Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUrl);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);



        try (Producer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            logger.info("Producer is created...");
            String line;
            int recordCount = 0;

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
            LocalDateTime previousTimestamp = null;

            // Ignorare header, se presente
            if ((line = br.readLine()) != null && line.startsWith("date")) {
                System.out.println("Skipping header: " + line);
                line = br.readLine();
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                keepRunning = false;
                logger.info("Shutdown hook triggered. Flushing and closing producer.");
                producer.flush();
                producer.close();
            }));

            while (line != null) {
                String[] record = line.split(","); // Assuming CSV is comma-separated
                String key = record[0]; // Assuming the first field is a good key
                String value = line;

                try {
                    LocalDateTime currentTimestamp = LocalDateTime.parse(record[0], formatter);
                    if (previousTimestamp != null) {
                        long timeDiff = Duration.between(previousTimestamp, currentTimestamp).toMillis();
                        Thread.sleep(timeDiff / speedup); // Regolare il ritardo in base al fattore di velocizzazione
                    }
                    previousTimestamp = currentTimestamp;

                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                    producer.send(producerRecord);

                    if (++recordCount % 10000 == 0) {
                        System.out.printf("Sent %d records%n", recordCount);
                    }

                    producer.flush();
                } catch (DateTimeParseException e) {
                    System.err.println("Failed to parse timestamp: " + record[0]);
                    e.printStackTrace();
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.err.println("Malformed record: " + line);
                    e.printStackTrace();
                }

                line = br.readLine();
            }

            producer.flush();
            logger.info("Finished publishing records");

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static void downloadFile(String url, String localFilename) throws IOException {
        Path path = Paths.get(localFilename);
        if (Files.exists(path)) {
            System.out.println("File already exists: " + localFilename);
            return;
        }

        try (InputStream in = new URL(url).openStream()) {
            Files.copy(in, path);
            System.out.println("Downloaded file: " + localFilename);
        } catch (IOException e) {
            System.err.println("Failed to download file: " + e.getMessage());
            throw e;
        }
    }

    private static void extractTarGz(String tarGzPath, String outputPath) throws IOException {
        try (FileInputStream fis = new FileInputStream(tarGzPath);
             GzipCompressorInputStream gzis = new GzipCompressorInputStream(fis);
             TarArchiveInputStream tais = new TarArchiveInputStream(gzis)) {

            TarArchiveEntry entry;
            while ((entry = tais.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                Path outputFilePath = Paths.get(outputPath);
                try (OutputStream os = Files.newOutputStream(outputFilePath, StandardOpenOption.CREATE)) {
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = tais.read(buffer)) != -1) {
                        os.write(buffer, 0, bytesRead);
                    }
                }
            }
            System.out.println("Extracted file: " + outputPath);
        } catch (IOException e) {
            System.err.println("Failed to extract file: " + e.getMessage());
            throw e;
        }
    }
}
