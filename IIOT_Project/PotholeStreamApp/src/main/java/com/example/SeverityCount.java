package com.example;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class SeverityCount {

    private Map<String, String> tags;
    private long count;

    public SeverityCount() {
        this.tags = new HashMap<>();
    }
    
    // --- Getters and Setters ---
    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = tags; }
    
    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }

    // --- Convenience Methods ---
    @JsonIgnore // Prevents this from being a duplicate field in the JSON
    public String getSeverityType() {
        return this.tags.get("severityType");
    }

    public void setSeverityType(String severityType) {
        this.tags.put("severityType", severityType);
    }
    
    @Override
    public String toString() {
        return "SeverityCount{" + "tags=" + tags + ", count=" + count + '}';
    }
}