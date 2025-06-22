package com.example; // Or your actual package

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.HashMap;
import java.util.Map;

public class PotholeState {
    // This MUST have a public getTags() method for serialization
    private Map<String, String> tags;

    // These will become InfluxDB fields
    private long firstSeenTimestampMs;
    private long lastReportedTimestampMs;
    private double currentSeverityValue;
    private int detectionCount;
    private double latitude;
    private double longitude;
    // We will keep locationId and severityType as fields, but ALSO add them to the tags map

    // Default constructor for Jackson
    public PotholeState() {
        this.tags = new HashMap<>();
    }

    public PotholeState(String locationId, String severityType, long firstSeenTimestampMs, long lastReportedTimestampMs,
                        double currentSeverityValue, int detectionCount, double latitude, double longitude) {
        this(); // This initializes the tags map
        this.firstSeenTimestampMs = firstSeenTimestampMs;
        this.lastReportedTimestampMs = lastReportedTimestampMs;
        this.currentSeverityValue = currentSeverityValue;
        this.detectionCount = detectionCount;
        this.latitude = latitude;
        this.longitude = longitude;

        // ** CRITICAL: Populate the tags map **
        if (locationId != null) {
            this.tags.put("locationId", locationId);
        }
        if (severityType != null) {
            this.tags.put("severityType", severityType);
        }
    }

    // *** ADD THESE METHODS BACK IN ***
    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    // Convenience Getters for easier access in Java code (optional but good practice)
    // We use @JsonIgnore to prevent these from becoming duplicate fields in the JSON output
    @JsonIgnore
    public String getLocationIdFromTags() {
        return this.tags != null ? this.tags.get("locationId") : null;
    }

    @JsonIgnore
    public String getSeverityTypeFromTags() {
        return this.tags != null ? this.tags.get("severityType") : null;
    }


    // Standard Getters and Setters for InfluxDB fields
    public long getFirstSeenTimestampMs() { return firstSeenTimestampMs; }
    public long getLastReportedTimestampMs() { return lastReportedTimestampMs; }
    public double getCurrentSeverityValue() { return currentSeverityValue; }
    public int getDetectionCount() { return detectionCount; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }

    public void setFirstSeenTimestampMs(long firstSeenTimestampMs) { this.firstSeenTimestampMs = firstSeenTimestampMs; }
    public void setLastReportedTimestampMs(long lastReportedTimestampMs) { this.lastReportedTimestampMs = lastReportedTimestampMs; }
    public void setCurrentSeverityValue(double currentSeverityValue) { this.currentSeverityValue = currentSeverityValue; }
    public void setDetectionCount(int detectionCount) { this.detectionCount = detectionCount; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }

    @Override
    public String toString() {
        return "PotholeState{" +
                "tags=" + tags + // Added tags to toString for better logging
                ", firstSeenTimestampMs=" + firstSeenTimestampMs +
                ", lastReportedTimestampMs=" + lastReportedTimestampMs +
                ", currentSeverityValue=" + currentSeverityValue +
                ", detectionCount=" + detectionCount +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}