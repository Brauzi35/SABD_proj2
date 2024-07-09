package com.sabd2.flink.query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class FailureAggregator implements AggregateFunction<Record, Tuple3<Integer, Integer, List<DiskFailure>>, Tuple3<Integer, Integer, List<DiskFailure>>> {
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