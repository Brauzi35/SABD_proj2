package com.sabd2.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class MetricRichMapFunction<T> extends RichMapFunction<T, T> {
    private transient double throughput = 0;
    private transient double latency = 0;
    private transient long counter = 0;
    private transient double start;
    private transient PrintWriter writer;
    private String windowId;

    public MetricRichMapFunction(String windowId) {
        this.windowId = windowId;
    }

    @Override
    public void open(Configuration config) {
        System.out.println("OPEN METRICS FOR " + windowId);

        getRuntimeContext().getMetricGroup().gauge("throughput", (Gauge<Double>) () -> this.throughput);
        getRuntimeContext().getMetricGroup().gauge("latency", (Gauge<Double>) () -> this.latency);
        this.start = System.currentTimeMillis();

        try {

            FileWriter fileWriter = new FileWriter("metrics.txt", true);
            writer = new PrintWriter(fileWriter);
            System.out.println("Metrics file created/opened successfully for " + windowId);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error opening the metrics file for " + windowId + ": " + e.getMessage());
        }

    }

    @Override
    public void close() {
        if (writer != null) {
            writer.close();
            System.out.println("Metrics file closed for " + windowId);
        }
    }

    @Override
    public T map(T value) {
        this.counter++;
        double elapsed_millis = System.currentTimeMillis() - this.start;
        double elapsed_sec = elapsed_millis / 1000;
        this.throughput = this.counter / elapsed_sec; // tuple / s
        this.latency = elapsed_millis / this.counter; // ms / tuple


        System.out.println("Processing element #" + this.counter + " for window " + windowId);


        if (writer != null) {
            writer.println("Window " + windowId + " - Throughput: " + this.throughput + " tuples/s");
            writer.println("Window " + windowId + " - Latency: " + this.latency + " ms/tuple");
            writer.flush();
            System.out.println("Metrics written to file.");
        }

        return value;
    }
}