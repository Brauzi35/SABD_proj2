package com.sabd2.flink.query2;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class RecordParser {
    public static Record parseRecord(String record) {
        try {
            //System.out.println("Raw Record: " + record);
            String[] fields = record.split(",", -1);
            if (fields.length < 26) { // Ensure the number of fields is correct
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
}