package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Flink Streaming Job that processes data from Kafka topics
 * This job demonstrates:
 * 1. Reading from multiple Kafka topics
 * 2. JSON parsing and data transformation
 * 3. Fraud detection logic
 * 4. Real-time analytics with windowing
 */
public class KafkaStreamingJob {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set parallelism for local testing
        env.setParallelism(2);
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(5000);

        // Create Kafka source for transactions
        KafkaSource<String> transactionSource = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("transactions")
            .setGroupId("flink-transaction-consumer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Create Kafka source for user activities
        KafkaSource<String> activitySource = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("user_activities")
            .setGroupId("flink-activity-consumer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Create transaction stream
        DataStream<String> transactionStream = env
            .fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Kafka Transaction Source");

        // Create activity stream
        DataStream<String> activityStream = env
            .fromSource(activitySource, WatermarkStrategy.noWatermarks(), "Kafka Activity Source");

        // Process transactions
        DataStream<Transaction> parsedTransactions = transactionStream
            .map(new TransactionParser())
            .filter(transaction -> transaction != null);

        // Print all transactions
        parsedTransactions.print("📦 Transactions");

        // Fraud detection
        DataStream<String> fraudAlerts = parsedTransactions
            .filter(new FraudDetectionFilter())
            .map(transaction -> String.format("🚨 FRAUD ALERT: Transaction %s by user %s for $%.2f flagged as fraudulent!", 
                transaction.transactionId, transaction.userId, transaction.totalAmount));

        fraudAlerts.print("🔴 Fraud Alerts");

        // High value transactions
        DataStream<String> highValueTransactions = parsedTransactions
            .filter(transaction -> transaction.totalAmount > 500.0)
            .map(transaction -> String.format("💰 High Value: $%.2f transaction by %s (%s)", 
                transaction.totalAmount, transaction.userId, transaction.category));

        highValueTransactions.print("💰 High Value Transactions");

        // Sales analytics - revenue per category in 30-second windows
        DataStream<Tuple3<String, Integer, Double>> categoryAnalytics = parsedTransactions
            .map(transaction -> new Tuple3<>(transaction.category, 1, transaction.totalAmount))
            .keyBy(value -> value.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
            .process(new CategoryAnalyticsProcessor());

        categoryAnalytics.print("📊 Category Analytics (30s window)");

        // Process user activities
        DataStream<UserActivity> parsedActivities = activityStream
            .map(new ActivityParser())
            .filter(activity -> activity != null);

        // Print user activities
        parsedActivities.print("👤 User Activities");

        // Execute the job
        env.execute("Kafka Streaming Analytics Job");
    }

    // Data classes
    public static class Transaction {
        public String transactionId;
        public String userId;
        public String userSegment;
        public String productId;
        public String productName;
        public String category;
        public double price;
        public int quantity;
        public double totalAmount;
        public String paymentMethod;
        public String country;
        public String city;
        public String timestamp;
        public boolean isFraud;

        public Transaction() {}

        @Override
        public String toString() {
            return String.format("Transaction{id='%s', user='%s', amount=$%.2f, category='%s'}", 
                transactionId, userId, totalAmount, category);
        }
    }

    public static class UserActivity {
        public String eventId;
        public String userId;
        public String sessionId;
        public String activityType;
        public String pageUrl;
        public String timestamp;
        public int durationSeconds;

        public UserActivity() {}

        @Override
        public String toString() {
            return String.format("Activity{user='%s', type='%s', duration=%ds}", 
                userId, activityType, durationSeconds);
        }
    }

    // Parser functions
    public static class TransactionParser implements MapFunction<String, Transaction> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Transaction map(String jsonString) {
            try {
                JsonNode jsonNode = objectMapper.readTree(jsonString);
                Transaction transaction = new Transaction();
                
                transaction.transactionId = jsonNode.get("transaction_id").asText();
                transaction.userId = jsonNode.get("user_id").asText();
                transaction.userSegment = jsonNode.get("user_segment").asText();
                transaction.productId = jsonNode.get("product_id").asText();
                transaction.productName = jsonNode.get("product_name").asText();
                transaction.category = jsonNode.get("category").asText();
                transaction.price = jsonNode.get("price").asDouble();
                transaction.quantity = jsonNode.get("quantity").asInt();
                transaction.totalAmount = jsonNode.get("total_amount").asDouble();
                transaction.paymentMethod = jsonNode.get("payment_method").asText();
                transaction.country = jsonNode.get("country").asText();
                transaction.city = jsonNode.get("city").asText();
                transaction.timestamp = jsonNode.get("timestamp").asText();
                transaction.isFraud = jsonNode.get("is_fraud").asBoolean();
                
                return transaction;
            } catch (Exception e) {
                System.err.println("Error parsing transaction: " + e.getMessage());
                return null;
            }
        }
    }

    public static class ActivityParser implements MapFunction<String, UserActivity> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public UserActivity map(String jsonString) {
            try {
                JsonNode jsonNode = objectMapper.readTree(jsonString);
                UserActivity activity = new UserActivity();
                
                activity.eventId = jsonNode.get("event_id").asText();
                activity.userId = jsonNode.get("user_id").asText();
                activity.sessionId = jsonNode.get("session_id").asText();
                activity.activityType = jsonNode.get("activity_type").asText();
                activity.pageUrl = jsonNode.get("page_url").asText();
                activity.timestamp = jsonNode.get("timestamp").asText();
                activity.durationSeconds = jsonNode.get("duration_seconds").asInt();
                
                return activity;
            } catch (Exception e) {
                System.err.println("Error parsing activity: " + e.getMessage());
                return null;
            }
        }
    }

    // Business logic functions
    public static class FraudDetectionFilter implements FilterFunction<Transaction> {
        @Override
        public boolean filter(Transaction transaction) {
            // Multiple fraud detection rules
            return transaction.isFraud || 
                   transaction.totalAmount > 2000.0 || 
                   (transaction.totalAmount > 1000.0 && transaction.userSegment.equals("new"));
        }
    }

    public static class CategoryAnalyticsProcessor extends ProcessWindowFunction<Tuple3<String, Integer, Double>, Tuple3<String, Integer, Double>, String, TimeWindow> {
        @Override
        public void process(String category, Context context, Iterable<Tuple3<String, Integer, Double>> elements, Collector<Tuple3<String, Integer, Double>> out) {
            int totalTransactions = 0;
            double totalRevenue = 0.0;
            
            for (Tuple3<String, Integer, Double> element : elements) {
                totalTransactions += element.f1;
                totalRevenue += element.f2;
            }
            
            out.collect(new Tuple3<>(category, totalTransactions, totalRevenue));
        }
    }
}
