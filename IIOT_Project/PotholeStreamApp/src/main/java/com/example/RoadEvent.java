package com.example; // Or your actual package

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

public class RoadEvent {

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("event_timestamp")
    private String timestampString; // Raw ISO 8601 timestamp string

    @JsonProperty("device_id")
    private String deviceId;

    @JsonProperty("location_latitude")
    private double latitude;

    @JsonProperty("location_longitude")
    private double longitude;

    @JsonProperty("accelerometer_x")
    private double accelerometerX;

    @JsonProperty("accelerometer_y")
    private double accelerometerY;

    @JsonProperty("accelerometer_z")
    private double accelerometerZ;

    @JsonProperty("severity") // The raw 0.0-1.0 float from input
    private double severityFloat;

    @JsonProperty("severity_type") // <<< CHANGED from severity_category
    private String rawSeverityType; // <<< CHANGED from rawSeverityCategory

    @JsonProperty("location_id")
    private String locationId;

    // Default constructor for Jackson deserialization
    public RoadEvent() {
    }

    // --- Getters ---
    public String getEventType() { return eventType; }
    public String getTimestampString() { return timestampString; }
    public String getDeviceId() { return deviceId; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getAccelerometerX() { return accelerometerX; }
    public double getAccelerometerY() { return accelerometerY; }
    public double getAccelerometerZ() { return accelerometerZ; }
    public double getSeverityFloat() { return severityFloat; }
    public String getRawSeverityType() { return rawSeverityType; } // <<< CHANGED
    public String getLocationId() { return locationId; }

    // --- Setters ---
    public void setEventType(String eventType) { this.eventType = eventType; }
    public void setTimestampString(String timestampString) { this.timestampString = timestampString; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    public void setAccelerometerX(double accelerometerX) { this.accelerometerX = accelerometerX; }
    public void setAccelerometerY(double accelerometerY) { this.accelerometerY = accelerometerY; }
    public void setAccelerometerZ(double accelerometerZ) { this.accelerometerZ = accelerometerZ; }
    public void setSeverityFloat(double severityFloat) { this.severityFloat = severityFloat; }
    public void setRawSeverityType(String rawSeverityType) { this.rawSeverityType = rawSeverityType; } // <<< CHANGED
    public void setLocationId(String locationId) { this.locationId = locationId; }

    /**
     * Parses the ISO 8601 timestamp string to epoch seconds (double for sub-second precision).
     */
    @JsonIgnore
    public double getTimestampEpochSeconds() {
        if (this.timestampString == null || this.timestampString.isEmpty()) {
            return 0.0;
        }
        try {
            OffsetDateTime odt = OffsetDateTime.parse(this.timestampString);
            return odt.toEpochSecond() + (odt.getNano() / 1_000_000_000.0);
        } catch (DateTimeParseException e) {
            System.err.println(">>> RoadEvent POJO: Failed to parse timestamp string: '" + this.timestampString + "' - " + e.getMessage());
            return 0.0;
        }
    }

    @Override
    public String toString() {
        return "RoadEvent{" +
                "eventType='" + eventType + '\'' +
                ", timestampString='" + timestampString + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", accelerometerX=" + accelerometerX +
                ", accelerometerY=" + accelerometerY +
                ", accelerometerZ=" + accelerometerZ +
                ", severityFloat=" + severityFloat +
                ", rawSeverityType='" + rawSeverityType + '\'' + // <<< CHANGED
                ", locationId='" + locationId + '\'' +
                '}';
    }
}