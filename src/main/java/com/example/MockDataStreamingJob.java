package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Instant;
import java.util.Random;

/**
 * Flink Streaming Job that processes mock sensor data
 * This job demonstrates:
 * 1. Mock data generation using DataGen connector
 * 2. Stream processing with windowing
 * 3. Aggregations and transformations
 */
public class MockDataStreamingJob {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set parallelism for local testing
        env.setParallelism(2);
        
        // Enable checkpointing for fault tolerance (every 5 seconds)
        env.enableCheckpointing(5000);

        // Create a mock sensor data generator
        DataGeneratorSource<SensorReading> sensorSource = 
            new DataGeneratorSource<>(
                new SensorDataGenerator(),
                1000, // Generate 1000 records per second
                null  // Generate indefinitely
            );

        // Create the sensor data stream
        DataStream<SensorReading> sensorStream = env
            .fromSource(sensorSource, WatermarkStrategy.noWatermarks(), "Sensor Data Generator")
            .setParallelism(1);

        // Print raw sensor data
        sensorStream.print("Raw Sensor Data");

        // Transform and aggregate the data
        DataStream<Tuple2<String, Double>> avgTemperatureStream = sensorStream
            .map(new TemperatureMapper())
            .keyBy(value -> value.f0) // Key by sensor ID
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 10-second windows
            .reduce(new TemperatureAverager());

        // Print aggregated results
        avgTemperatureStream.print("Average Temperature per Sensor (10s window)");

        // Create alerts for high temperatures
        DataStream<String> alertStream = sensorStream
            .filter(reading -> reading.temperature > 80.0)
            .map(reading -> String.format("🚨 ALERT: High temperature detected! Sensor %s: %.2f°C at %s", 
                reading.sensorId, reading.temperature, reading.timestamp));

        alertStream.print("Temperature Alerts");

        // Execute the job
        env.execute("Mock Data Streaming Job");
    }

    /**
     * Data class representing a sensor reading
     */
    public static class SensorReading {
        public String sensorId;
        public double temperature;
        public double humidity;
        public String timestamp;
        public String location;

        public SensorReading() {}

        public SensorReading(String sensorId, double temperature, double humidity, String timestamp, String location) {
            this.sensorId = sensorId;
            this.temperature = temperature;
            this.humidity = humidity;
            this.timestamp = timestamp;
            this.location = location;
        }

        @Override
        public String toString() {
            return String.format("SensorReading{sensorId='%s', temp=%.2f°C, humidity=%.2f%%, location='%s', time='%s'}", 
                sensorId, temperature, humidity, location, timestamp);
        }
    }

    /**
     * Generator function for creating mock sensor data
     */
    public static class SensorDataGenerator implements GeneratorFunction<Long, SensorReading> {
        private final Random random = new Random();
        private final String[] sensorIds = {"sensor-001", "sensor-002", "sensor-003", "sensor-004", "sensor-005"};
        private final String[] locations = {"Building-A", "Building-B", "Building-C", "Warehouse", "DataCenter"};

        @Override
        public SensorReading map(Long index) {
            String sensorId = sensorIds[random.nextInt(sensorIds.length)];
            double temperature = 15 + random.nextGaussian() * 20; // Normal distribution around 15°C
            double humidity = 30 + random.nextDouble() * 40; // 30-70% humidity
            String timestamp = Instant.now().toString();
            String location = locations[random.nextInt(locations.length)];
            
            return new SensorReading(sensorId, temperature, humidity, timestamp, location);
        }
    }

    /**
     * Maps SensorReading to Tuple2 for aggregation
     */
    public static class TemperatureMapper implements MapFunction<SensorReading, Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> map(SensorReading reading) {
            return new Tuple2<>(reading.sensorId, reading.temperature);
        }
    }

    /**
     * Calculates average temperature for each sensor
     */
    public static class TemperatureAverager implements ReduceFunction<Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) {
            return new Tuple2<>(value1.f0, (value1.f1 + value2.f1) / 2.0);
        }
    }
}
