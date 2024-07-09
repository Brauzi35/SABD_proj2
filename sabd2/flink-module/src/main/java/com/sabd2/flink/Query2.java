package com.sabd2.flink;

import com.sabd2.flink.MetricRichMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query2 {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

    public static void main(String[] args) throws Exception {

        // setup
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
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

        // timestamps and watermarks
        DataStream<Record> parsedStream = stream
                .map(Query2::parseRecord)
                .filter(record -> record != null)
                .filter(record -> record.getFailure() == 1) // Ensure only records with failures are processed
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Record>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
                                .withIdleness(Duration.ofMinutes(5))

                );


        // aggregate failures per vault in time windows of 1 day
        DataStream<Tuple3<Integer, Integer, List<DiskFailure>>> failureStream1Day = parsedStream
                .keyBy(Record::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new FailureAggregator())
                .map(new MetricRichMapFunction<>("1 Day"));

        // aggregate failures per vault in time windows of 3 days
        DataStream<Tuple3<Integer, Integer, List<DiskFailure>>> failureStream3Days = parsedStream
                .keyBy(Record::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(3),Time.days(2)))
                .aggregate(new FailureAggregator())
                .map(new MetricRichMapFunction<>("3 Day"));

        // aggregate failures per vault in a global window (all time)
        DataStream<Tuple3<Integer, Integer, List<DiskFailure>>> failureStreamAllTime = parsedStream
                .keyBy(Record::getVaultId)
                //.window(TumblingEventTimeWindows.of(Time.days(Long.MAX_VALUE)))
                .window(TumblingEventTimeWindows.of(Time.days(23),Time.days(13)))
                .aggregate(new FailureAggregator())
                .map(new MetricRichMapFunction<>("All time"));

        // top 10 vaults for each window size
        DataStream<String> top10Stream1Day = calculateTop10(failureStream1Day, Time.days(1),Time.days(0));
        DataStream<String> top10Stream3Days = calculateTop10(failureStream3Days, Time.days(3),Time.days(2));
        DataStream<String> top10StreamAllTime = calculateTop10(failureStreamAllTime, Time.days(23),Time.days(13));

        // output serialization schema for the Kafka sink
        SimpleStringSchema stringSchema = new SimpleStringSchema();

        // configure Kafka sink properties for different topics
        FlinkKafkaProducer<String> producer1Day = new FlinkKafkaProducer<>(
                "query2_1-day",                    // Target topic for 1 day window
                stringSchema,                // Serialization schema
                properties                  // Producer config
        );

        FlinkKafkaProducer<String> producer3Days = new FlinkKafkaProducer<>(
                "query2_3-days",                  // Target topic for 3 days window
                stringSchema,                // Serialization schema
                properties                  // Producer config
        );

        FlinkKafkaProducer<String> producerAllTime = new FlinkKafkaProducer<>(
                "query2_all-time",               // Target topic for all time window
                stringSchema,                // Serialization schema
                properties                  // Producer config
        );

        top10Stream1Day
                .map(result -> {
                    System.out.println("1 Day Window Result: " + result);
                    return result;
                })
                .addSink(producer1Day);

        top10Stream3Days
                .map(result -> {
                    System.out.println("3 Days Window Result: " + result);
                    return result;
                })
                .addSink(producer3Days);

        top10StreamAllTime
                .map(result -> {
                    System.out.println("All Time Window Result: " + result);
                    return result;
                })
                .addSink(producerAllTime);

        // exec
        env.execute("Kafka Flink Real-time Failure Ranking");
    }

    private static DataStream<String> calculateTop10(DataStream<Tuple3<Integer, Integer, List<DiskFailure>>> failureStream, Time windowSize, Time offset) {

        return failureStream
                .windowAll(TumblingEventTimeWindows.of(windowSize, offset))
                .apply(new AllWindowFunction<Tuple3<Integer, Integer, List<DiskFailure>>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple3<Integer, Integer, List<DiskFailure>>> values, Collector<String> out) {
                        List<Tuple3<Integer, Integer, List<DiskFailure>>> sortedFailures = StreamSupport.stream(values.spliterator(), false)
                                .sorted((a, b) -> Integer.compare(b.f1, a.f1))
                                .limit(10)
                                .collect(Collectors.toList());

                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

                        // start e end time della finestra in formato leggibile
                        String windowStart = formatter.format(Instant.ofEpochMilli(window.getStart()));
                        String windowEnd = formatter.format(Instant.ofEpochMilli(window.getEnd()));


                        System.out.println("Processing window: " + windowStart + " to " + windowEnd);

                        StringBuilder resultBuilder = new StringBuilder();
                        resultBuilder.append(windowStart).append(", ");

                        for (Tuple3<Integer, Integer, List<DiskFailure>> failure : sortedFailures) {
                            resultBuilder.append(", ")
                                    .append(failure.f0)
                                    .append(", ")
                                    .append(failure.f1);

                            if (!failure.f2.isEmpty()) {
                                resultBuilder.append(" (");
                                resultBuilder.append(failure.f2.stream()
                                        .map(DiskFailure::toString)
                                        .collect(Collectors.joining(", ")));
                                resultBuilder.append(")");
                            } else {
                                resultBuilder.append(" (No failures)");
                            }
                        }

                        String result = resultBuilder.toString();
                        out.collect(result);
                    }
                });

    }



    // parse incoming Kafka records
    private static Record parseRecord(String record) {
        try {
            //System.out.println("Raw Record: " + record);
            String[] fields = record.split(",", -1);
            if (fields.length < 26) { // Assicurarsi che il numero di campi sia corretto
                System.err.println("Invalid record (insufficient fields): " + record);
                return null;
            }

            String timestampString = fields[0].replace("T", " ");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            LocalDateTime dateTime;
            try {
                dateTime = LocalDateTime.parse(timestampString, formatter);
            } catch (DateTimeParseException e) {
                System.err.println("Error parsing timestamp in record: " + record);
                return null;
            }

            Integer vaultId;
            try {
                vaultId = Integer.parseInt(fields[4]);
            } catch (NumberFormatException e) {
                System.err.println("Non-numeric vaultId: " + fields[4]);
                return null;
            }

            Integer failure;
            try {
                failure = Integer.parseInt(fields[3]);
            } catch (NumberFormatException e) {
                System.err.println("Non-numeric failure: " + fields[3]);
                return null;
            }


            Record parsedRecord = new Record();
            parsedRecord.setTimestamp(Timestamp.valueOf(timestampString));
            parsedRecord.setVaultId(vaultId);
            parsedRecord.setFailure(failure);

            DiskFailure diskFailure = new DiskFailure(fields[2], fields[1]);
            parsedRecord.addDiskFailure(diskFailure);

            //System.out.println("Parsed Record: " + parsedRecord);
            return parsedRecord;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error parsing record: " + record);
            return null;
        }
    }


    // data structure
    public static class Record {
        private int vaultId;
        private Timestamp timestamp;
        private List<DiskFailure> diskFailures = new ArrayList<>();
        private int failure;

        public int getVaultId() {
            return vaultId;
        }

        public void setVaultId(int vaultId) {
            this.vaultId = vaultId;
        }

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }

        public List<DiskFailure> getDiskFailures() {
            return diskFailures;
        }

        public void setDiskFailures(List<DiskFailure> diskFailures) {
            this.diskFailures = diskFailures;
        }

        public void addDiskFailure(DiskFailure diskFailure) {
            this.diskFailures.add(diskFailure);
        }
        public int getFailure() {
            return failure;
        }

        public void setFailure(int failure) {
            this.failure = failure;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "vaultId=" + vaultId +
                    ", timestamp=" + timestamp +
                    ", diskFailures=" + diskFailures +
                    ", failure=" + failure +
                    '}';
        }
    }

    // details of a disk failure
    public static class DiskFailure {
        private String model;
        private String serialNumber;

        public DiskFailure(String model, String serialNumber) {
            this.model = model;
            this.serialNumber = serialNumber;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public String getSerialNumber() {
            return serialNumber;
        }

        public void setSerialNumber(String serialNumber) {
            this.serialNumber = serialNumber;
        }

        @Override
        public String toString() {
            return "[" +
                    "model='" + model + '\'' +
                    ", serialNumber='" + serialNumber + '\'' +
                    ']';
        }
    }

    // count failures and collect disk failure details
    public static class FailureAggregator implements AggregateFunction<Record, Tuple3<Integer, Integer, List<DiskFailure>>, Tuple3<Integer, Integer, List<DiskFailure>>> {
        @Override
        public Tuple3<Integer, Integer, List<DiskFailure>> createAccumulator() {
            Tuple3<Integer, Integer, List<DiskFailure>> accumulator = new Tuple3<>(0, 0, new ArrayList<>());
            //System.out.println("Created new accumulator: " + accumulator);
            return accumulator;
        }

        @Override
        public Tuple3<Integer, Integer, List<DiskFailure>> add(Record value, Tuple3<Integer, Integer, List<DiskFailure>> accumulator) {
            //System.out.println("Adding value to accumulator: " + value);
            accumulator.f0 = value.getVaultId();
            accumulator.f1 += value.getFailure();
            accumulator.f2.addAll(value.getDiskFailures());
            //System.out.println("Updated accumulator: " + accumulator);
            return accumulator;
        }

        @Override
        public Tuple3<Integer, Integer, List<DiskFailure>> getResult(Tuple3<Integer, Integer, List<DiskFailure>> accumulator) {
            //System.out.println("Final result from accumulator: " + accumulator);
            return accumulator;
        }

        @Override
        public Tuple3<Integer, Integer, List<DiskFailure>> merge(Tuple3<Integer, Integer, List<DiskFailure>> a, Tuple3<Integer, Integer, List<DiskFailure>> b) {
            //System.out.println("Merging accumulators: " + a + " and " + b);
            a.f1 += b.f1;
            a.f2.addAll(b.f2);
            //System.out.println("Merged accumulator: " + a);
            return a;
        }
    }
}
