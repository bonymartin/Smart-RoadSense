package com.example; // Or your actual package

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class AggregatedStats {
    // 1. Add the map for tags. This will be visible in the JSON output.
    private Map<String, String> tags;

    // These will become InfluxDB "fields"
    private long windowStartMs;
    private long windowEndMs;
    private int totalPotholes;
    private double sumSeverity;
    private double avgSeverity;
    // NOTE: The 'private String areaKey;' field is no longer needed.
    // The value will be stored inside the 'tags' map instead.

    public AggregatedStats() {
        // Initialize the map in the default constructor.
        this.tags = new HashMap<>();
    }

    public AggregatedStats(String areaKey, long windowStartMs, long windowEndMs, int totalPotholes, double sumSeverity) {
        this(); // This calls the default constructor to create the HashMap.
        
        // Use our new setter to populate the tags map.
        this.setAreaKey(areaKey);
        
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.totalPotholes = totalPotholes;
        this.sumSeverity = sumSeverity;
        this.recalculateAvgSeverity(); // Use the helper method for consistency.
    }

    // --- GETTERS & SETTERS ---

    // The public getter for the 'tags' map. This is what makes it appear in the JSON.
    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    // 2. Add @JsonIgnore to the getter for 'areaKey'.
    // This allows your Java code to call 'getAreaKey()' as before,
    // but prevents Jackson from creating a duplicate "areaKey" field in the JSON.
    @JsonIgnore
    public String getAreaKey() {
        // 3. Change the getter to retrieve the value FROM the map.
        return this.tags.get("areaKey");
    }

    // 4. Change the setter to store the value INTO the map.
    public void setAreaKey(String areaKey) {
        this.tags.put("areaKey", areaKey);
    }

    // --- The rest of your getters and setters remain the same ---
    public long getWindowStartMs() { return windowStartMs; }
    public void setWindowStartMs(long windowStartMs) { this.windowStartMs = windowStartMs; }
    
    public long getWindowEndMs() { return windowEndMs; }
    public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }
    
    public int getTotalPotholes() { return totalPotholes; }
    public void setTotalPotholes(int totalPotholes) { this.totalPotholes = totalPotholes; }
    
    public double getSumSeverity() { return sumSeverity; }
    public void setSumSeverity(double sumSeverity) { this.sumSeverity = sumSeverity; }
    
    public double getAvgSeverity() { return avgSeverity; }
    public void setAvgSeverity(double avgSeverity) { this.avgSeverity = avgSeverity; }

    // --- Business logic methods remain the same ---
    public void recalculateAvgSeverity() {
        if (this.totalPotholes > 0) {
            this.avgSeverity = this.sumSeverity / this.totalPotholes;
        } else {
            this.avgSeverity = 0.0;
        }
    }

    @Override
    public String toString() {
        // 5. Update toString() to be more informative for logging.
        return "AggregatedStats{" +
               "tags=" + tags + // Print the tags map
               ", windowStartMs=" + windowStartMs +
               ", windowEndMs=" + windowEndMs +
               ", totalPotholes=" + totalPotholes +
               ", sumSeverity=" + sumSeverity +
               ", avgSeverity=" + String.format("%.2f", avgSeverity) +
               '}';
    }
}