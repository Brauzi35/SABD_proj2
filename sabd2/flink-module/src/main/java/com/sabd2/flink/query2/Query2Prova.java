package com.sabd2.flink.query2;

import com.sabd2.flink.MetricRichMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query2Prova {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Configure Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-consumer-group");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("max.poll.interval.ms", "300000"); // 5 minutes

        // Create Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "input-data",
                new SimpleStringSchema(),
                properties
        );

        // Assign the consumer to the execution environment
        DataStream<String> stream = env.addSource(consumer);

        // Extract timestamps and generate watermarks
        DataStream<Record> parsedStream = stream
                .map(RecordParser::parseRecord)
                .filter(record -> record != null)
                .filter(record -> record.getFailure() == 1) // Ensure only records with failures are processed
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Record>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
                                .withIdleness(Duration.ofMinutes(5))
                );

        // Aggregate failures per vault in time windows of 1 day
        DataStream<Tuple3<Integer, Integer, List<DiskFailure>>> failureStream1Day = parsedStream
                .keyBy(Record::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new FailureAggregator())
                .map(new MetricRichMapFunction<>("1 Day"));

        // Aggregate failures per vault in time windows of 3 days
        DataStream<Tuple3<Integer, Integer, List<DiskFailure>>> failureStream3Days = parsedStream
                .keyBy(Record::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(3), Time.days(2)))
                .aggregate(new FailureAggregator())
                .map(new MetricRichMapFunction<>("3 Day"));

        // Aggregate failures per vault in a global window (all time)
        DataStream<Tuple3<Integer, Integer, List<DiskFailure>>> failureStreamAllTime = parsedStream
                .keyBy(Record::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(23), Time.days(13)))
                .aggregate(new FailureAggregator())
                .map(new MetricRichMapFunction<>("All time"));

        // Calculate top 10 vaults for each window size
        DataStream<String> top10Stream1Day = calculateTop10(failureStream1Day, Time.days(1), Time.days(0));
        DataStream<String> top10Stream3Days = calculateTop10(failureStream3Days, Time.days(3), Time.days(2));
        DataStream<String> top10StreamAllTime = calculateTop10(failureStreamAllTime, Time.days(23), Time.days(13));

        // Define the output serialization schema for the Kafka sink
        SimpleStringSchema stringSchema = new SimpleStringSchema();

        // Configure Kafka sink properties for different topics
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

        // Execute the program
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

                        // Reinitialize DateTimeFormatter inside the method
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

                        // Convert window start and end time to readable format
                        String windowStart = formatter.format(Instant.ofEpochMilli(window.getStart()));
                        String windowEnd = formatter.format(Instant.ofEpochMilli(window.getEnd()));

                        // Log to indicate the time interval of the window
                        System.out.println("Processing window: " + windowStart + " to " + windowEnd);

                        StringBuilder resultBuilder = new StringBuilder();
                        resultBuilder.append(windowStart).append(", ").append(windowEnd);

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
}