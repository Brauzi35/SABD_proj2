package com.sabd2.flink.query2;
public class DiskFailure {
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