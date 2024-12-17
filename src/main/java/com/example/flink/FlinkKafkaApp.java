package com.example.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;


import java.util.Properties;

public class FlinkKafkaApp {
    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Confluent Cloud configuration
        String bootstrapServers = "pkc-921jm.us-east-2.aws.confluent.cloud:9092";  // Update with your bootstrap server URL
        String sourceTopic = "SourceTopic";  // Update with your source Kafka topic
        String destinationTopic = "DestinationTopic";  // Update with your destination Kafka topic
        String apiKey = "TD56B2VJCUYQAYOG";  // Update with your Confluent Cloud API key
        String apiSecret = "L3H4XnpfZXWT9sX7Gm+64MvqYKbmQR1QBPP5OiRqVLop47OECns8Va6o48cRWZL0";  // Update with your Confluent Cloud API secret
        String schemaRegistryEndpoint = "https://psrc-8kz20.us-east-2.aws.confluent.cloud";
        String srapiCreds = "RIOHS5EPA6A225ID:KAGCG/MEdR/fWAKdi5DE9xghGTjsn2GMjRojbDyNmXqaI6mpRWzi5IMxsZREaAT6";

        // Kafka properties for Confluent Cloud (SASL/SSL authentication)
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + apiKey + "' password='" + apiSecret + "';");
        properties.put("schema.registry.url", schemaRegistryEndpoint);
        properties.put("schema.registry.basic.auth.credentials.source", "USER_INFO");
        properties.put("schema.registry.basic.auth.user.info", srapiCreds);


        // Create KafkaSource for Confluent Cloud
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperties(properties)  // Set Kafka properties for authentication
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create a data stream from Kafka source
        DataStream<String> sourceStream = env.fromSource(kafkaSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Apply a transformation: Convert the input message to uppercase (for example)
        DataStream<String> transformedStream = sourceStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        // Set up KafkaSink to write to Confluent Cloud

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092");
        producerProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"TD56B2VJCUYQAYOG\" password=\"L3H4XnpfZXWT9sX7Gm+64MvqYKbmQR1QBPP5OiRqVLop47OECns8Va6o48cRWZL0\";");
        producerProps.put("security.protocol", "SASL_SSL");
        producerProps.put("sasl.mechanism", "PLAIN");

        KafkaSink<String> kafkaSink1 = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(destinationTopic)  // Specify the destination topic
                        .setValueSerializationSchema(new SimpleStringSchema())  // Serializer for the messages
                        .build())
                .build();

        // Write transformed data to the Kafka destination topic
        transformedStream.sinkTo(kafkaSink1);

        // Execute the Flink job
        env.execute("Flink Kafka Transformation Job");
    }
}
