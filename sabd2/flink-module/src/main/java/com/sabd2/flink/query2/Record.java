package com.sabd2.flink.query2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class Record {
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