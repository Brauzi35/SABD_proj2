package com.sabd2.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import com.tdunning.math.stats.TDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class Query3 {
    private static final Logger logger = LoggerFactory.getLogger(Query3.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-consumer-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "input-data",
                new SimpleStringSchema(),
                properties
        );

        DataStream<String> stream = env.addSource(consumer);

        DataStream<PowerOnHoursRecord> recordsStream = stream
                .map(Query3::parseRecord)
                .filter(Objects::nonNull)
                .filter(record -> record.getVaultId() >= 1090 && record.getVaultId() <= 1120)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PowerOnHoursRecord>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
                );

        DataStream<Tuple8<String, Integer, Long, Double, Double, Double, Double, Double>> results1Day = recordsStream
                .keyBy(PowerOnHoursRecord::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new LatestPowerOnHoursAggregator(), new WindowResultFunction())
                .map(new MetricRichMapFunction<>("1 Day"));

        DataStream<Tuple8<String, Integer, Long, Double, Double, Double, Double, Double>> results3Day = recordsStream
                .keyBy(PowerOnHoursRecord::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(3), Time.days(2)))
                .aggregate(new LatestPowerOnHoursAggregator(), new WindowResultFunction())
                .map(new MetricRichMapFunction<>("3 Day"));

        DataStream<Tuple8<String, Integer, Long, Double, Double, Double, Double, Double>> resultsAllTime = recordsStream
                .keyBy(PowerOnHoursRecord::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(23), Time.days(13)))
                .aggregate(new LatestPowerOnHoursAggregator(), new WindowResultFunction())
                .map(new MetricRichMapFunction<>("All time"));

        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "kafka:9092");

        FlinkKafkaProducer<String> producer1Day = new FlinkKafkaProducer<>(
                "query3_1-day",
                new SimpleStringSchema(),
                producerProperties
        );

        FlinkKafkaProducer<String> producer3Days = new FlinkKafkaProducer<>(
                "query3_3-days",
                new SimpleStringSchema(),
                properties
        );

        FlinkKafkaProducer<String> producerAllTime = new FlinkKafkaProducer<>(
                "query3_all-time",
                new SimpleStringSchema(),
                properties
        );

        results1Day
                .map(Tuple8::toString)
                .addSink(producer1Day);

        results3Day
                .map(Tuple8::toString)
                .addSink(producer3Days);

        resultsAllTime
                .map(Tuple8::toString)
                .addSink(producerAllTime);

        env.execute("Query3");
    }

    private static PowerOnHoursRecord parseRecord(String record) {
        try {
            String[] fields = record.split(",", -1);
            if (fields.length < 26) {
                logger.error("Record invalido (numero di campi insufficiente): " + record);
                return null;
            }

            String timestampString = fields[0].replace("T", " ");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneId.of("UTC"));
            LocalDateTime dateTime;
            try {
                dateTime = LocalDateTime.parse(timestampString, formatter);
            } catch (DateTimeParseException e) {
                logger.error("Errore nel parsing del campo timestamp nel record: " + record, e);
                return null;
            }

            Integer vaultId;
            try {
                vaultId = Integer.parseInt(fields[4]);
            } catch (NumberFormatException e) {
                logger.error("Campo vaultId non numerico: " + fields[4], e);
                return null;
            }

            String serial = fields[2];
            if (serial.isEmpty()) {
                logger.error("Campo serial vuoto: " + fields[2]);
                return null;
            }

            Double powerOnHours;
            try {
                if (fields[12].isEmpty()) {
                    logger.error("Campo powerOnHours vuoto: " + fields[12]);
                    return null;
                }
                powerOnHours = Double.parseDouble(fields[12]);
            } catch (NumberFormatException e) {
                logger.error("Campo powerOnHours non numerico: " + fields[12]);
                return null;
            }

            PowerOnHoursRecord parsedRecord = new PowerOnHoursRecord();
            parsedRecord.setTimestamp(Timestamp.valueOf(timestampString));
            parsedRecord.setVaultId(vaultId);
            parsedRecord.setSerial(serial);
            parsedRecord.setPowerOnHours(powerOnHours);
            logger.debug("Parsed record: " + parsedRecord);
            return parsedRecord;
        } catch (Exception e) {
            logger.error("Errore nel parsing del record: " + record, e);
            return null;
        }
    }

    public static class LatestPowerOnHoursAggregator implements AggregateFunction<PowerOnHoursRecord, Map<String, PowerOnHoursRecord>, List<PowerOnHoursRecord>> {
        @Override
        public Map<String, PowerOnHoursRecord> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, PowerOnHoursRecord> add(PowerOnHoursRecord value, Map<String, PowerOnHoursRecord> accumulator) {
            accumulator.put(value.getSerial(), value);
            return accumulator;
        }

        @Override
        public List<PowerOnHoursRecord> getResult(Map<String, PowerOnHoursRecord> accumulator) {
            return new ArrayList<>(accumulator.values());
        }

        @Override
        public Map<String, PowerOnHoursRecord> merge(Map<String, PowerOnHoursRecord> a, Map<String, PowerOnHoursRecord> b) {
            for (Map.Entry<String, PowerOnHoursRecord> entry : b.entrySet()) {
                a.merge(entry.getKey(), entry.getValue(), (oldValue, newValue) -> oldValue.getTimestamp().after(newValue.getTimestamp()) ? oldValue : newValue);
            }
            return a;
        }
    }

    public static class WindowResultFunction implements WindowFunction<List<PowerOnHoursRecord>, Tuple8<String, Integer, Long, Double, Double, Double, Double, Double>, Integer, TimeWindow> {
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

        @Override
        public void apply(Integer key, TimeWindow window, Iterable<List<PowerOnHoursRecord>> input, Collector<Tuple8<String, Integer, Long, Double, Double, Double, Double, Double>> out) {
            List<PowerOnHoursRecord> records = input.iterator().next();

            TDigest digest = TDigest.createDigest(100.0);
            for (PowerOnHoursRecord record : records) {
                digest.add(record.getPowerOnHours());
            }

            double min = digest.quantile(0.0);
            double p25 = digest.quantile(0.25);
            double p50 = digest.quantile(0.50);
            double p75 = digest.quantile(0.75);
            double max = digest.quantile(1.0);
            long count = digest.size();

            String windowStart = formatter.format(Instant.ofEpochMilli(window.getStart()));

            out.collect(new Tuple8<>(windowStart, key, count, min, p25, p50, p75, max));
        }
    }

    public static class PowerOnHoursRecord {
        private Timestamp timestamp;
        private int vaultId;
        private String serial;
        private double powerOnHours;

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }

        public int getVaultId() {
            return vaultId;
        }

        public void setVaultId(int vaultId) {
            this.vaultId = vaultId;
        }

        public String getSerial() {
            return serial;
        }

        public void setSerial(String serial) {
            this.serial = serial;
        }

        public double getPowerOnHours() {
            return powerOnHours;
        }

        public void setPowerOnHours(double powerOnHours) {
            this.powerOnHours = powerOnHours;
        }

        @Override
        public String toString() {
            return "PowerOnHoursRecord{" +
                    "timestamp=" + timestamp +
                    ", vaultId=" + vaultId +
                    ", serial='" + serial + '\'' +
                    ", powerOnHours=" + powerOnHours +
                    '}';
        }
    }
}
