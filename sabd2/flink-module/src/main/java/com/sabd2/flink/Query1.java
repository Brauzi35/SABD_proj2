package com.sabd2.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.Properties;

public class Query1 {
    public static void main(String[] args) throws Exception {
        // setup
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092"); // Updated to match Docker setup
        properties.setProperty("group.id", "flink-consumer-group");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("max.poll.interval.ms", "300000"); // 5 minutes


        // consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "input-data",
                new SimpleStringSchema(),
                properties
        );

        DataStream<String> stream = env.addSource(consumer);

        // streams and watermarks
        DataStream<Record> parsedStream = stream
                .map(Query1::parseRecord)
                .filter(record -> record != null)
                .filter(record -> record.getVaultId() >= 1000 && record.getVaultId() <= 1020)
                .filter(record -> record.getTemperature() != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Record>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
                                .withIdleness(Duration.ofMinutes(5))
                );

        // aggregate results in time windows of 1 day
        DataStream<Tuple5<String, Integer, Long, Double, Double>> resultStream1Day = parsedStream
                .keyBy(Record::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new TemperatureStatisticsAggregator(), new WindowResultFunction())
                .map(new MetricRichMapFunction<>("1 Day"));

        // aggregate results in time windows of 3 days
        DataStream<Tuple5<String, Integer, Long, Double, Double>> resultStream3Days = parsedStream
                .keyBy(Record::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(3),Time.days(2)))
                .aggregate(new TemperatureStatisticsAggregator(), new WindowResultFunction())
                .map(new MetricRichMapFunction<>("3 Day"));


        // aggregate results from the beginning of the dataset
        DataStream<Tuple5<String, Integer, Long, Double, Double>> resultStreamAllTime = parsedStream
                .keyBy(Record::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(23),Time.days(13)))
                .aggregate(new TemperatureStatisticsAggregator(), new WindowResultFunction())
                .map(new MetricRichMapFunction<>("All time"));



        SimpleStringSchema stringSchema = new SimpleStringSchema();

        // Kafka sink properties for different topics
        FlinkKafkaProducer<String> producer1Day = new FlinkKafkaProducer<>(
                "query1-1day",                    // Target topic for 1 day window
                stringSchema,                // Serialization schema
                properties                  // Producer config
        );


        FlinkKafkaProducer<String> producer3Days = new FlinkKafkaProducer<>(
                "query1-3days",                  // Target topic for 3 days window
                stringSchema,                // Serialization schema
                properties                  // Producer config
        );

        FlinkKafkaProducer<String> producerAllTime = new FlinkKafkaProducer<>(
                "query1-all-time",               // Target topic for all time window
                stringSchema,                // Serialization schema
                properties                  // Producer config
        );


        resultStream1Day
                .map(result -> {
                    //System.out.println("1 Day Window Result: " + result);
                    return result.toString();
                })
                .addSink(producer1Day);


        resultStream3Days
                .map(result -> {
                    //System.out.println("3 Days Window Result: " + result);
                    return result.toString();
                })
                .addSink(producer3Days);

        resultStreamAllTime
                .map(result -> {
                    //System.out.println("All Time Window Result: " + result);
                    return result.toString();
                })
                .addSink(producerAllTime);

        // execute
        env.execute("Kafka Flink Consumer");
    }

    // parse incoming Kafka records
    private static Record parseRecord(String record) {
        try {
            // dividere il record in campi, inclusi quelli vuoti
            String[] fields = record.split(",", -1); // Utilizzare -1 per includere campi vuoti
            if (fields.length < 26) { // Assicurarsi che ci siano abbastanza campi
                System.err.println("Record invalido (numero di campi insufficiente): " + record);
                return null; // Il record non è valido
            }

            // parsing del campo timestamp
            String timestampString = fields[0].replace("T", " "); // Il timestamp è il primo campo e sostituiamo 'T' con uno spazio
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"); // Modificato il pattern per riflettere la sostituzione
            LocalDateTime dateTime;
            try {
                dateTime = LocalDateTime.parse(timestampString, formatter);
            } catch (DateTimeParseException e) {
                System.err.println("Errore nel parsing del campo timestamp nel record: " + record);
                return null;
            }
            //long epochSeconds = dateTime.toEpochSecond(ZoneOffset.UTC);
            //System.out.println("TEMPO: " + timestampString);


            // parsing del campo vaultId (quinto campo, indice 4)
            Integer vaultId;
            try {
                vaultId = Integer.parseInt(fields[4]); // Il vaultId è il quinto campo
            } catch (NumberFormatException e) {
                System.err.println("Campo vaultId non numerico: " + fields[4]);
                return null;
            }

            // parsing del campo temperatura (26esimo campo, indice 25 corrispondente a s194_temperature_celsius)
            Double temperature;
            if (fields[25].isEmpty()) {
                //System.err.println("Campo temperatura vuoto: " + fields[25]);
                return null;
            }
            try {
                temperature = Double.parseDouble(fields[25]); // La temperatura è il ventiseiesimo campo
            } catch (NumberFormatException e) {
                System.err.println("Campo temperatura non numerico: " + fields[25]);
                return null;
            }

            Record parsedRecord = new Record();
            //parsedRecord.setTimestamp(epochSeconds);
            parsedRecord.setTimestamp(Timestamp.valueOf(timestampString));
            parsedRecord.setVaultId(vaultId);
            parsedRecord.setTemperature(temperature);
            return parsedRecord;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Errore nel parsing del record: " + record);
            return null;
        }
    }

    //data structure
    public static class Record {
        private int vaultId;
        private Double temperature;
        private Timestamp timestamp;

        public int getVaultId() {
            return vaultId;
        }

        public void setVaultId(int vaultId) {
            this.vaultId = vaultId;
        }

        public Double getTemperature() {
            return temperature;
        }

        public void setTemperature(Double temperature) {
            this.temperature = temperature;
        }

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }
    }

    //aggregated results
    public static class Statistics {
        private int vaultId;
        private long eventCount;
        private double meanTemperature;
        private double stdDevTemperature;

        public int getVaultId() {
            return vaultId;
        }

        public void setVaultId(int vaultId) {
            this.vaultId = vaultId;
        }

        public long getEventCount() {
            return eventCount;
        }

        public void setEventCount(long eventCount) {
            this.eventCount = eventCount;
        }

        public double getMeanTemperature() {
            return meanTemperature;
        }

        public void setMeanTemperature(double meanTemperature) {
            this.meanTemperature = meanTemperature;
        }

        public double getStdDevTemperature() {
            return stdDevTemperature;
        }

        public void setStdDevTemperature(double stdDevTemperature) {
            this.stdDevTemperature = stdDevTemperature;
        }

        // rappresentazione leggibile
        @Override
        public String toString() {
            return "Statistics{" +
                    "vaultId=" + vaultId +
                    ", eventCount=" + eventCount +
                    ", meanTemperature=" + meanTemperature +
                    ", stdDevTemperature=" + stdDevTemperature +
                    '}';
        }
    }

    // Aggregator class using Welford's algorithm
    public static class TemperatureStatisticsAggregator implements AggregateFunction<Record, WelfordAccumulator, Statistics> {
        @Override
        public WelfordAccumulator createAccumulator() {
            WelfordAccumulator accumulator = new WelfordAccumulator();
            //System.out.println("Creato nuovo accumulatore: " + accumulator); ne crea tanti, va bene?
            return accumulator;
        }

        @Override
        public WelfordAccumulator add(Record value, WelfordAccumulator accumulator) {
            //System.out.println("Aggiunta del valore: " + value.getTemperature() + " all'accumulatore con VaultId: " + value.getVaultId());
            accumulator.setVaultId(value.getVaultId());
            accumulator.add(value.getTemperature());
            //System.out.println("Stato accumulatore aggiornato: " + accumulator);
            return accumulator;
        }

        @Override
        public Statistics getResult(WelfordAccumulator accumulator) {
            Statistics stats = new Statistics();
            stats.setVaultId(accumulator.getVaultId());
            stats.setEventCount(accumulator.getCount());
            stats.setMeanTemperature(accumulator.getMean());
            stats.setStdDevTemperature(accumulator.getStdDev());
            //System.out.println("Risultato finale calcolato: " + stats);
            return stats;
        }

        @Override
        public WelfordAccumulator merge(WelfordAccumulator a, WelfordAccumulator b) {
            //System.out.println("Merging accumulatori: " + a + " e " + b);
            WelfordAccumulator merged = a.merge(b);
            //System.out.println("Risultato del merge: " + merged);
            return merged;
        }
    }

    // Welford's algorithm accumulator class
    public static class WelfordAccumulator {
        private int vaultId;
        private long count;
        private double mean;
        private double m2;

        public void add(double value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            m2 += delta * delta2;
        }

        public WelfordAccumulator merge(WelfordAccumulator other) {
            if (other.count == 0) return this;
            if (this.count == 0) return other;
            long totalCount = this.count + other.count;
            double delta = other.mean - this.mean;
            this.mean += delta * other.count / totalCount;
            this.m2 += other.m2 + delta * delta * this.count * other.count / totalCount;
            this.count = totalCount;
            return this;
        }

        public long getCount() {
            return count;
        }

        public double getMean() {
            return mean;
        }

        public double getStdDev() {
            return count > 1 ? Math.sqrt(m2 / (count - 1)) : 0.0;
        }

        public int getVaultId() {
            return vaultId;
        }

        public void setVaultId(int vaultId) {
            this.vaultId = vaultId;
        }
    }

    // Statistics class definition

    // WindowResultFunction class definition
    //processo i dati all'interno di una finestra temporale,
    // estraendo le informazioni rilevanti e emettendo i risultati aggregati
    /*public static class WindowResultFunction implements WindowFunction<Statistics, Tuple5<Long, Integer, Long, Double, Double>, Integer, TimeWindow> {
        @Override
        public void apply(Integer key, TimeWindow window, Iterable<Statistics> input, Collector<Tuple5<Long, Integer, Long, Double, Double>> out) {
            Statistics statistics = input.iterator().next();
            out.collect(new Tuple5<>(window.getStart(), statistics.getVaultId(), statistics.getEventCount(), statistics.getMeanTemperature(), statistics.getStdDevTemperature()));
        }
    }*/
    public static class WindowResultFunction implements WindowFunction<Statistics, Tuple5<String, Integer, Long, Double, Double>, Integer, TimeWindow> {
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

        @Override
        public void apply(Integer key, TimeWindow window, Iterable<Statistics> input, Collector<Tuple5<String, Integer, Long, Double, Double>> out) {
            // converti gli start e end time della finestra in formato leggibile
            String windowStart = formatter.format(Instant.ofEpochMilli(window.getStart()));
            String windowEnd = formatter.format(Instant.ofEpochMilli(window.getEnd()));


            System.out.println("Processing window: " + windowStart + " to " + windowEnd);

            Iterator<Statistics> iterator = input.iterator();
            if (!iterator.hasNext()) {
                System.err.println("L'iterabile di input è vuoto");
                return;
            }

            while (iterator.hasNext()) {
                Statistics statistics = iterator.next();
                if (statistics != null) {
                    //System.out.println("Oggetto Statistics trovato: " + statistics);
                    out.collect(new Tuple5<>(
                            windowStart,
                            statistics.getVaultId(),
                            statistics.getEventCount(),
                            statistics.getMeanTemperature(),
                            statistics.getStdDevTemperature()
                    ));
                    System.out.println("Dati emessi: " + windowStart + ", " + statistics.getVaultId() + ", " + statistics.getEventCount() + ", " + statistics.getMeanTemperature() + ", " + statistics.getStdDevTemperature());
                } else {
                    System.err.println("L'oggetto Statistics è null");
                }
            }
        }
    }


}
