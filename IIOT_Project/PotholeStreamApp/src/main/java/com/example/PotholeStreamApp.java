package com.example; // Or actual package

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class PotholeStreamApp {

    private static final String RAW_EVENTS_TOPIC = "pothole-events";
    private static final String DEDUPLICATED_EVENTS_TOPIC = "deduplicated-pothole-events";
    private static final String UNIQUE_POTHOLE_FIRST_OCCURRENCE_TOPIC = "unique_pothole_first_reports";
    private static final String AGGREGATED_STATS_TOPIC = "weekly_pothole_summary_stats";
    private static final String SEVERITY_TYPE_DISTRIBUTION_TOPIC = "pothole_severity_type_distribution_counts";
    private static final int AREA_PRECISION = 3;

    public static String getAreaKey(PotholeState state) {
        if (state == null) return "UNKNOWN_AREA";
        String formatString = "AREA_%." + AREA_PRECISION + "f_%." + AREA_PRECISION + "f";
        return String.format(formatString, state.getLatitude(), state.getLongitude());
    }

    public static void createTopics(Properties props) throws ExecutionException, InterruptedException {
        // ... topic creation logic remains the same ...
        System.out.println(">>> PRE-CREATE_TOPICS: Checking and creating topics...");
        Properties adminProps = new Properties();
        adminProps.putAll(props);
        adminProps.remove(StreamsConfig.APPLICATION_ID_CONFIG);
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            for (NewTopic topic : Arrays.asList(
                new NewTopic(RAW_EVENTS_TOPIC, 1, (short) 1),
                new NewTopic(DEDUPLICATED_EVENTS_TOPIC, 1, (short) 1),
                new NewTopic(UNIQUE_POTHOLE_FIRST_OCCURRENCE_TOPIC, 1, (short) 1),
                new NewTopic(AGGREGATED_STATS_TOPIC, 1, (short) 1),
                new NewTopic(SEVERITY_TYPE_DISTRIBUTION_TOPIC, 1, (short) 1)
            )) {
                if (!existingTopics.contains(topic.name())) {
                    try {
                        adminClient.createTopics(Collections.singletonList(topic)).all().get();
                    } catch (InterruptedException | ExecutionException e) {
                        if (!(e.getCause() instanceof TopicExistsException)) { System.err.println(">>> PRE-CREATE_TOPICS_ERROR: " + e.getMessage()); }
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        // IMPORTANT: Use a new application ID to reset state for the severity distribution logic.
        props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/tmp/kafka-streams");   // Store Kafka state locally
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pothole-processor-final-severity-v9");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Use your correct broker address
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        try {
            createTopics(props);
        } catch (Exception e) {
            System.err.println(">>> FATAL_ERROR_PRE_CREATE: Could not create/verify topics.");
            e.printStackTrace(System.err);
            System.exit(1);
        }

        final Serde<PotholeState> potholeStateSerde = new JsonPojoSerde<>(PotholeState.class);
        final Serde<AggregatedStats> aggregatedStatsSerde = new JsonPojoSerde<>(AggregatedStats.class);
        final Serde<RoadEvent> roadEventSerde = new JsonPojoSerde<>(RoadEvent.class);
        // *** CHANGE 1: Define the new Serde ***
        final Serde<SeverityCount> severityCountSerde = new JsonPojoSerde<>(SeverityCount.class);

        StreamsBuilder builder = new StreamsBuilder();
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        KStream<String, String> rawJsonInput = builder.stream(RAW_EVENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, RoadEvent> roadEvents = rawJsonInput.flatMapValues(value -> {
            try {
                RoadEvent event = objectMapper.readValue(value, RoadEvent.class);
                if (event.getLocationId() == null || !"pothole_detected".equals(event.getEventType())) {
                    return Collections.emptyList();
                }
                return Collections.singletonList(event);
            } catch (Exception e) {
                e.printStackTrace(System.err);
                return Collections.emptyList();
            }
        });

        KGroupedStream<String, RoadEvent> groupedByLocation = roadEvents.groupBy(
            (key, roadEvent) -> roadEvent.getLocationId(), Grouped.with(Serdes.String(), roadEventSerde));
            
        KTable<String, PotholeState> potholeStateTable = groupedByLocation.aggregate(
            () -> null,
            (locationKey, newEvent, currentPotholeState) -> {
                double newSeverityFloat = newEvent.getSeverityFloat();
                String newSeverityType = newEvent.getRawSeverityType();
                long eventTimestampMs = (long) (newEvent.getTimestampEpochSeconds() * 1000);
                if (currentPotholeState == null) {
                    return new PotholeState(locationKey, newSeverityType, eventTimestampMs, eventTimestampMs,
                        newSeverityFloat, 1, newEvent.getLatitude(), newEvent.getLongitude());
                } else {
                    currentPotholeState.setDetectionCount(currentPotholeState.getDetectionCount() + 1);
                    currentPotholeState.setLastReportedTimestampMs(eventTimestampMs);
                    if (newSeverityFloat >= currentPotholeState.getCurrentSeverityValue()) {
                        currentPotholeState.setCurrentSeverityValue(newSeverityFloat);
                        currentPotholeState.getTags().put("severityType", newSeverityType);
                    }
                    return currentPotholeState;
                }
            },
            Materialized.<String, PotholeState, KeyValueStore<Bytes, byte[]>>as("pothole-state-store-final-v9")
                .withKeySerde(Serdes.String()).withValueSerde(potholeStateSerde)
        );

        KStream<String, PotholeState> updatedPotholeStatesStream = potholeStateTable.toStream();
        updatedPotholeStatesStream.to(DEDUPLICATED_EVENTS_TOPIC, Produced.with(Serdes.String(), potholeStateSerde));
        
        KStream<String, PotholeState> uniqueNewPotholesStream = updatedPotholeStatesStream
            .filter((key, value) -> value != null && value.getDetectionCount() == 1);
        uniqueNewPotholesStream.to(UNIQUE_POTHOLE_FIRST_OCCURRENCE_TOPIC, Produced.with(Serdes.String(), potholeStateSerde));
        
        // *** CHANGE 2: Replace the entire severityTypeDistributionTable logic ***
        System.out.println(">>> TOPOLOGY: Defining KTable for severity type distribution...");
        KTable<String, SeverityCount> severityTypeDistributionTable = potholeStateTable
            .groupBy((locationKey, potholeStateValue) -> {
                String type = (potholeStateValue != null && potholeStateValue.getSeverityTypeFromTags() != null)
                            ? potholeStateValue.getSeverityTypeFromTags() : "UNKNOWN";
                return KeyValue.pair(type, potholeStateValue);
            }, Grouped.with(Serdes.String(), potholeStateSerde))
            .aggregate(
                () -> new SeverityCount(),
                (severityType, potholeState, aggregate) -> { // Adder
                    aggregate.setCount(aggregate.getCount() + 1);
                    aggregate.setSeverityType(severityType);
                    return aggregate;
                },
                (severityType, potholeState, aggregate) -> { // Subtractor
                    aggregate.setCount(aggregate.getCount() - 1);
                    aggregate.setSeverityType(severityType);
                    return aggregate;
                },
                Materialized.<String, SeverityCount, KeyValueStore<Bytes, byte[]>>
                    as("severity-type-counts-final-v9")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(severityCountSerde) // Use the new Serde
            );
        
        // Stream the results to the topic using the new Serde
        severityTypeDistributionTable.toStream()
            .to(SEVERITY_TYPE_DISTRIBUTION_TOPIC, Produced.with(Serdes.String(), severityCountSerde));
        System.out.println(">>> TOPOLOGY: Severity type distribution counts KTable defined and sending to " + SEVERITY_TYPE_DISTRIBUTION_TOPIC);

        // --- Weekly Aggregation Logic remains the same ---
        KStream<String, PotholeState> sourceForWeeklyAggregation = builder.stream(
            UNIQUE_POTHOLE_FIRST_OCCURRENCE_TOPIC, Consumed.with(Serdes.String(), potholeStateSerde));
        KTable<Windowed<String>, AggregatedStats> weeklyUniquePotholeCounts = sourceForWeeklyAggregation
            .groupBy((potholeKey, potholeStateValue) -> getAreaKey(potholeStateValue), Grouped.with(Serdes.String(), potholeStateSerde))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(7)))
            .aggregate(
                () -> new AggregatedStats(),
                (areaKey, newUniquePothole, aggregate) -> {
                    if (aggregate.getAreaKey() == null) aggregate.setAreaKey(areaKey);
                    aggregate.setTotalPotholes(aggregate.getTotalPotholes() + 1);
                    aggregate.setSumSeverity(aggregate.getSumSeverity() + newUniquePothole.getCurrentSeverityValue());
                    aggregate.recalculateAvgSeverity();
                    return aggregate;
                },
                Materialized.<String, AggregatedStats, WindowStore<Bytes, byte[]>>
                    as("weekly-unique-counts-final-v9")
                    .withKeySerde(Serdes.String()).withValueSerde(aggregatedStatsSerde)
            );
        weeklyUniquePotholeCounts.toStream()
            .mapValues((windowedKey, value) -> {
                if (value != null && windowedKey != null && windowedKey.window() != null) {
                    value.setWindowStartMs(windowedKey.window().start());
                    value.setWindowEndMs(windowedKey.window().end());
                }
                return value;
            })
            .filter((key, value) -> value != null)
            .to(AGGREGATED_STATS_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofDays(7).toMillis()), aggregatedStatsSerde));
        
        // --- STARTUP AND SHUTDOWN ---
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (streams != null && streams.state().isRunningOrRebalancing()) {
                streams.close(Duration.ofSeconds(30));
            }
        }));

        try {
            System.out.println(">>> MAIN: Cleaning up local state stores...");
            streams.cleanUp();
            System.out.println(">>> MAIN: Starting Pothole Processing Pipeline...");
            streams.start();
            Thread.currentThread().join();
        } catch (final Throwable e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}